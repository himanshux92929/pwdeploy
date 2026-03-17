import os
import json
import uuid
import asyncio
import logging
import tempfile
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message,
)
from pyrogram.errors import FloodWait, ChannelPrivate, UsernameNotOccupied, ChatAdminRequired
from supabase import create_client, Client as SupabaseClient

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
API_ID        = int(os.environ.get("API_ID", "0"))
API_HASH      = os.environ.get("API_HASH", "")
BOT_TOKEN     = os.environ.get("BOT_TOKEN", "")
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")   # service-role key

DEFAULT_OWNER = 7380969878

# ─────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────
sb: SupabaseClient = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────

def get_owners() -> list[int]:
    try:
        res = sb.table("owners").select("user_id").execute()
        ids = [row["user_id"] for row in res.data]
        return ids if ids else [DEFAULT_OWNER]
    except Exception as e:
        log.error(f"get_owners error: {e}")
        return [DEFAULT_OWNER]


def add_owner(user_id: int):
    sb.table("owners").upsert({"user_id": user_id}).execute()


def remove_owner(user_id: int):
    sb.table("owners").delete().eq("user_id", user_id).execute()


def _normalize_task(row: dict) -> dict:
    row["delete_words"]  = row.get("delete_words") or []
    row["replace_words"] = row.get("replace_words") or {}
    return row


def get_tasks() -> dict:
    try:
        res = sb.table("tasks").select("*").execute()
        return {row["id"]: _normalize_task(row) for row in res.data}
    except Exception as e:
        log.error(f"get_tasks error: {e}")
        return {}


def get_task(task_id: str) -> dict | None:
    try:
        res = sb.table("tasks").select("*").eq("id", task_id).single().execute()
        return _normalize_task(res.data) if res.data else None
    except Exception as e:
        log.error(f"get_task({task_id}) error: {e}")
        return None


def save_task(task: dict):
    payload = {
        "id":            task["id"],
        "name":          task["name"],
        "source":        task["source"],
        "dest":          task["dest"],
        "last_id":       task.get("last_id", 0),
        "keep_caption":  task.get("keep_caption", True),
        "delete_words":  task.get("delete_words", []),
        "replace_words": task.get("replace_words", {}),
        "add_text":      task.get("add_text", ""),
        "enabled":       task.get("enabled", True),
    }
    sb.table("tasks").upsert(payload).execute()


def delete_task(task_id: str):
    sb.table("tasks").delete().eq("id", task_id).execute()


def update_last_id(task_id: str, new_id: int):
    sb.table("tasks").update({"last_id": new_id}).eq("id", task_id).execute()


def new_task_id() -> str:
    return str(uuid.uuid4())[:8]


def export_as_json() -> str:
    data = {"owners": get_owners(), "tasks": get_tasks()}
    return json.dumps(data, indent=2, ensure_ascii=False)


def import_from_json(raw: str):
    data = json.loads(raw)
    # Overwrite owners
    sb.table("owners").delete().neq("user_id", -1).execute()
    for uid in data.get("owners", [DEFAULT_OWNER]):
        add_owner(uid)
    # Overwrite tasks
    sb.table("tasks").delete().neq("id", "").execute()
    for task in data.get("tasks", {}).values():
        save_task(task)

# ─────────────────────────────────────────────
# CAPTION PROCESSOR
# ─────────────────────────────────────────────

def process_caption(caption: str | None, task: dict) -> str:
    if not task.get("keep_caption", True):
        return task.get("add_text", "") or ""

    text = caption or ""

    for old, new in (task.get("replace_words") or {}).items():
        text = text.replace(old, new)

    for word in (task.get("delete_words") or []):
        text = text.replace(word, "")

    add = task.get("add_text", "")
    if add:
        text = text.rstrip() + f"\n\n{add}"

    return text.strip()

# ─────────────────────────────────────────────
# PYROGRAM CLIENT
# ─────────────────────────────────────────────
app = Client(
    "forwarder_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

user_states: dict[int, dict] = {}

# ─────────────────────────────────────────────
# OWNER FILTER
# ─────────────────────────────────────────────
async def _is_owner(_, __, update):
    uid = update.from_user.id if update.from_user else None
    return uid in get_owners()

owner_filter = filters.create(_is_owner)

# ─────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View / Manage Tasks", callback_data="view_tasks")],
        [InlineKeyboardButton("➕ Add New Task",         callback_data="add_task")],
        [InlineKeyboardButton("👥 Manage Owners",        callback_data="manage_owners")],
        [InlineKeyboardButton("📤 Export JSON",          callback_data="export_json")],
        [InlineKeyboardButton("📥 Import JSON",          callback_data="import_json")],
    ])


def kb_task_list(tasks: dict):
    buttons = []
    for tid, t in tasks.items():
        icon = "✅" if t.get("enabled", True) else "❌"
        buttons.append([InlineKeyboardButton(f"{icon} {t['name']}", callback_data=f"task_{tid}")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(buttons)


def kb_task_detail(task: dict):
    tid     = task["id"]
    enabled = task.get("enabled", True)
    cap     = task.get("keep_caption", True)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'🔴 Disable' if enabled else '🟢 Enable'}", callback_data=f"toggle_{tid}"),
         InlineKeyboardButton("🗑 Delete Task",    callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("✏️ Rename",         callback_data=f"rename_{tid}"),
         InlineKeyboardButton("🔄 Change Source",  callback_data=f"chsrc_{tid}")],
        [InlineKeyboardButton("📬 Change Dest",    callback_data=f"chdst_{tid}"),
         InlineKeyboardButton(f"📝 Caption {'ON' if cap else 'OFF'}", callback_data=f"togglecap_{tid}")],
        [InlineKeyboardButton("🔢 Set Last ID",    callback_data=f"setlid_{tid}"),
         InlineKeyboardButton("➕ Add Text",        callback_data=f"addtext_{tid}")],
        [InlineKeyboardButton("🗑 Delete Words",   callback_data=f"delwords_{tid}"),
         InlineKeyboardButton("🔁 Replace Words",  callback_data=f"repwords_{tid}")],
        [InlineKeyboardButton("👁 View Filters",   callback_data=f"viewfilters_{tid}"),
         InlineKeyboardButton("▶️ Run Now",         callback_data=f"runnow_{tid}")],
        [InlineKeyboardButton("🔙 Back",           callback_data="view_tasks")],
    ])

# ─────────────────────────────────────────────
# COMMANDS
# ─────────────────────────────────────────────

@app.on_message(filters.command("start") & filters.private & owner_filter)
async def cmd_start(client, msg: Message):
    user_states.pop(msg.from_user.id, None)
    await msg.reply("👋 **Auto-Forwarder Control Panel**\n\nChoose an option:", reply_markup=kb_main())


@app.on_message(filters.command("cancel") & filters.private & owner_filter)
async def cmd_cancel(client, msg: Message):
    user_states.pop(msg.from_user.id, None)
    await msg.reply("❌ Cancelled.", reply_markup=kb_main())

# ─────────────────────────────────────────────
# CALLBACK ROUTER
# ─────────────────────────────────────────────

def _fake_cq(original: CallbackQuery, new_data: str) -> CallbackQuery:
    original.data = new_data
    return original


@app.on_callback_query(owner_filter)
async def cb_handler(client, cq: CallbackQuery):
    data = cq.data
    uid  = cq.from_user.id

    if data == "main_menu":
        user_states.pop(uid, None)
        await cq.message.edit_text("👋 **Control Panel** – Choose an option:", reply_markup=kb_main())

    elif data == "view_tasks":
        tasks = get_tasks()
        if not tasks:
            await cq.answer("No tasks yet. Add one first!", show_alert=True)
            return
        await cq.message.edit_text("📋 **Tasks** – tap one to manage:", reply_markup=kb_task_list(tasks))

    elif data.startswith("task_"):
        tid  = data[5:]
        task = get_task(tid)
        if not task:
            await cq.answer("Task not found!", show_alert=True); return
        text = (
            f"🔧 **Task: {task['name']}**\n\n"
            f"🔹 Source: `{task['source']}`\n"
            f"🔹 Dest: `{task['dest']}`\n"
            f"🔹 Last ID: `{task['last_id']}`\n"
            f"🔹 Status: {'✅ Enabled' if task.get('enabled', True) else '❌ Disabled'}\n"
            f"🔹 Caption: {'On' if task.get('keep_caption', True) else 'Off'}\n"
            f"🔹 Delete rules: {len(task.get('delete_words', []))}\n"
            f"🔹 Replace rules: {len(task.get('replace_words', {}))}\n"
            f"🔹 Add text: {'Yes' if task.get('add_text') else 'No'}"
        )
        await cq.message.edit_text(text, reply_markup=kb_task_detail(task))

    elif data.startswith("toggle_"):
        tid  = data[7:]; task = get_task(tid)
        if task:
            task["enabled"] = not task.get("enabled", True)
            save_task(task)
            await cq.answer(f"Task {'enabled ✅' if task['enabled'] else 'disabled ❌'}!")
            await cb_handler(client, _fake_cq(cq, f"task_{tid}"))

    elif data.startswith("togglecap_"):
        tid  = data[10:]; task = get_task(tid)
        if task:
            task["keep_caption"] = not task.get("keep_caption", True)
            save_task(task)
            await cq.answer(f"Caption {'on' if task['keep_caption'] else 'off'}!")
            await cb_handler(client, _fake_cq(cq, f"task_{tid}"))

    elif data.startswith("del_task_"):
        tid = data[9:]
        await cq.message.edit_text(
            "⚠️ Are you sure you want to **permanently delete** this task from Supabase?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, delete", callback_data=f"confirmdelete_{tid}"),
                 InlineKeyboardButton("❌ Cancel",       callback_data=f"task_{tid}")]
            ])
        )

    elif data.startswith("confirmdelete_"):
        tid = data[14:]
        delete_task(tid)
        await cq.answer("Task deleted!")
        tasks = get_tasks()
        if tasks:
            await cq.message.edit_text("📋 **Tasks:**", reply_markup=kb_task_list(tasks))
        else:
            await cq.message.edit_text("No tasks left.", reply_markup=kb_main())

    elif data.startswith("runnow_"):
        tid  = data[7:]; task = get_task(tid)
        if not task:
            await cq.answer("Task not found!", show_alert=True); return
        await cq.answer("▶️ Starting…")
        await cq.message.edit_text(f"⏳ Running task **{task['name']}**…")
        count, err = await run_task(client, task)
        result = f"✅ **{task['name']}** done.\nForwarded: **{count}** message(s)."
        if err:
            result += f"\n\n⚠️ Error: `{err}`"
        await cq.message.edit_text(result, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to task", callback_data=f"task_{tid}")]
        ]))

    elif data.startswith("viewfilters_"):
        tid  = data[12:]; task = get_task(tid)
        if not task:
            await cq.answer("Task not found!"); return
        dw = task.get("delete_words", [])
        rw = task.get("replace_words", {})
        at = task.get("add_text", "")
        text  = f"🔎 **Filters for '{task['name']}'**\n\n"
        text += ("**Delete words/phrases:**\n" + "\n".join(f"• `{w}`" for w in dw) + "\n\n") if dw else "**Delete words:** _none_\n\n"
        text += ("**Replace rules:**\n" + "\n".join(f"• `{k}` → `{v}`" for k, v in rw.items()) + "\n\n") if rw else "**Replace rules:** _none_\n\n"
        text += f"**Add text:** {f'`{at}`' if at else '_none_'}"
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Clear Delete Words", callback_data=f"cleardelwords_{tid}"),
             InlineKeyboardButton("🗑 Clear Replace",      callback_data=f"clearrep_{tid}")],
            [InlineKeyboardButton("🗑 Clear Add Text",     callback_data=f"clearaddtext_{tid}")],
            [InlineKeyboardButton("🔙 Back",               callback_data=f"task_{tid}")]
        ]))

    elif data.startswith("cleardelwords_"):
        tid  = data[14:]; task = get_task(tid)
        if task:
            task["delete_words"] = []; save_task(task); await cq.answer("Cleared!")
            await cb_handler(client, _fake_cq(cq, f"viewfilters_{tid}"))

    elif data.startswith("clearrep_"):
        tid  = data[9:]; task = get_task(tid)
        if task:
            task["replace_words"] = {}; save_task(task); await cq.answer("Cleared!")
            await cb_handler(client, _fake_cq(cq, f"viewfilters_{tid}"))

    elif data.startswith("clearaddtext_"):
        tid  = data[13:]; task = get_task(tid)
        if task:
            task["add_text"] = ""; save_task(task); await cq.answer("Cleared!")
            await cb_handler(client, _fake_cq(cq, f"viewfilters_{tid}"))

    elif data == "manage_owners":
        owners = get_owners()
        text   = "👥 **Owners:**\n" + "\n".join(f"• `{o}`" for o in owners)
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Owner",    callback_data="addowner")],
            [InlineKeyboardButton("🗑 Remove Owner", callback_data="removeowner")],
            [InlineKeyboardButton("🔙 Back",         callback_data="main_menu")]
        ]))

    elif data == "export_json":
        await cq.answer("Generating export…")
        json_str = export_as_json()
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w",
                                         encoding="utf-8", delete=False) as f:
            f.write(json_str)
            tmp_path = f.name
        await client.send_document(uid, tmp_path, caption="📦 **Supabase snapshot — data.json**")
        Path(tmp_path).unlink(missing_ok=True)

    elif data == "import_json":
        user_states[uid] = {"step": "import_json"}
        await cq.message.edit_text(
            "📥 Send me a **data.json** file to import into Supabase.\n\n/cancel to abort.",
            reply_markup=None
        )

    # ── Text-input triggers ──
    elif data == "add_task":
        user_states[uid] = {"step": "task_name"}
        await cq.message.edit_text("📝 **New Task – Step 1/4**\n\nSend the **task name**.\n\n/cancel to abort.")

    elif data == "addowner":
        user_states[uid] = {"step": "add_owner"}
        await cq.message.edit_text("👤 Send the **User ID** to add as owner.\n\n/cancel to abort.")

    elif data == "removeowner":
        user_states[uid] = {"step": "remove_owner"}
        await cq.message.edit_text("👤 Send the **User ID** to remove.\n\n/cancel to abort.")

    elif data.startswith("rename_"):
        tid = data[7:]; user_states[uid] = {"step": "rename", "tid": tid}
        await cq.message.edit_text("✏️ Send the **new task name**.\n\n/cancel to abort.")

    elif data.startswith("chsrc_"):
        tid = data[6:]; user_states[uid] = {"step": "chsrc", "tid": tid}
        await cq.message.edit_text("📡 Send the **new source** channel username or ID.\n\n/cancel to abort.")

    elif data.startswith("chdst_"):
        tid = data[6:]; user_states[uid] = {"step": "chdst", "tid": tid}
        await cq.message.edit_text("📬 Send the **new destination** channel username or ID.\n\n/cancel to abort.")

    elif data.startswith("setlid_"):
        tid = data[7:]; user_states[uid] = {"step": "setlid", "tid": tid}
        await cq.message.edit_text("🔢 Send the **Last Message ID** (integer). Use `0` to re-forward all.\n\n/cancel to abort.")

    elif data.startswith("addtext_"):
        tid = data[8:]; user_states[uid] = {"step": "addtext", "tid": tid}
        await cq.message.edit_text("➕ Send **text to append** to every caption. Send `-` to clear.\n\n/cancel to abort.")

    elif data.startswith("delwords_"):
        tid = data[9:]; user_states[uid] = {"step": "delwords", "tid": tid}
        await cq.message.edit_text(
            "🗑 Send **words/phrases to delete** from captions.\n"
            "• One entry per line — multi-line phrases supported.\n"
            "• Added to existing list.\n\n/cancel to abort."
        )

    elif data.startswith("repwords_"):
        tid = data[9:]; user_states[uid] = {"step": "repwords", "tid": tid}
        await cq.message.edit_text(
            "🔁 Send **replacement rules**, one per line:\n"
            "`old text => new text`\n\n"
            "Examples:\n`@spam => @legit`\n`BadWord => `\n\n"
            "(empty right side = delete)\n\n/cancel to abort."
        )

# ─────────────────────────────────────────────
# TEXT MESSAGE HANDLER
# ─────────────────────────────────────────────

@app.on_message(filters.private & owner_filter & ~filters.command(["start", "cancel"]))
async def handle_text(client, msg: Message):
    uid   = msg.from_user.id
    state = user_states.get(uid)

    if not state:
        await msg.reply("Use /start to open the Control Panel.")
        return

    step = state.get("step")

    if step == "import_json":
        if msg.document and msg.document.file_name.endswith(".json"):
            path = await msg.download()
            try:
                raw = Path(path).read_text(encoding="utf-8")
                import_from_json(raw)
                user_states.pop(uid, None)
                await msg.reply("✅ Imported into Supabase successfully!", reply_markup=kb_main())
            except Exception as e:
                await msg.reply(f"❌ Failed: {e}", reply_markup=kb_main())
            finally:
                Path(path).unlink(missing_ok=True)
        else:
            await msg.reply("Please send a **.json** file.")
        return

    if step == "add_owner":
        try:
            add_owner(int(msg.text.strip()))
            user_states.pop(uid, None)
            await msg.reply(f"✅ Owner `{msg.text.strip()}` added.", reply_markup=kb_main())
        except ValueError:
            await msg.reply("❌ Invalid ID.")
        return

    if step == "remove_owner":
        try:
            remove_owner(int(msg.text.strip()))
            user_states.pop(uid, None)
            await msg.reply(f"✅ Owner `{msg.text.strip()}` removed.", reply_markup=kb_main())
        except ValueError:
            await msg.reply("❌ Invalid ID.")
        return

    # Task wizard
    if step == "task_name":
        state["task_name"] = msg.text.strip(); state["step"] = "task_source"
        await msg.reply(f"✅ Name: `{state['task_name']}`\n\n**Step 2/4** – Send the **Source Channel**:")

    elif step == "task_source":
        state["source"] = msg.text.strip(); state["step"] = "task_dest"
        await msg.reply(f"✅ Source: `{state['source']}`\n\n**Step 3/4** – Send the **Destination Channel**:")

    elif step == "task_dest":
        state["dest"] = msg.text.strip(); state["step"] = "task_caption"
        await msg.reply(f"✅ Dest: `{state['dest']}`\n\n**Step 4/4** – Keep captions? Reply **yes** or **no**:")

    elif step == "task_caption":
        ans = msg.text.strip().lower()
        if ans not in ("yes", "no", "y", "n"):
            await msg.reply("Please reply **yes** or **no**."); return
        keep = ans in ("yes", "y")
        tid  = new_task_id()
        task = {
            "id": tid, "name": state["task_name"],
            "source": state["source"], "dest": state["dest"],
            "last_id": 0, "keep_caption": keep,
            "delete_words": [], "replace_words": {}, "add_text": "", "enabled": True,
        }
        save_task(task)
        user_states.pop(uid, None)
        await msg.reply(
            f"✅ **Task `{task['name']}` saved to Supabase!**\n"
            f"ID: `{tid}` | Caption: {'On' if keep else 'Off'}",
            reply_markup=kb_main()
        )

    elif step == "rename":
        task = get_task(state["tid"])
        if task:
            task["name"] = msg.text.strip(); save_task(task)
        user_states.pop(uid, None)
        await msg.reply("✅ Task renamed.", reply_markup=kb_main())

    elif step == "chsrc":
        task = get_task(state["tid"])
        if task:
            task["source"] = msg.text.strip(); save_task(task)
        user_states.pop(uid, None)
        await msg.reply("✅ Source updated.", reply_markup=kb_main())

    elif step == "chdst":
        task = get_task(state["tid"])
        if task:
            task["dest"] = msg.text.strip(); save_task(task)
        user_states.pop(uid, None)
        await msg.reply("✅ Destination updated.", reply_markup=kb_main())

    elif step == "setlid":
        try:
            new_id = int(msg.text.strip())
            update_last_id(state["tid"], new_id)
            user_states.pop(uid, None)
            await msg.reply(f"✅ Last ID set to `{new_id}`.", reply_markup=kb_main())
        except ValueError:
            await msg.reply("❌ Please send a valid integer.")

    elif step == "addtext":
        task = get_task(state["tid"])
        if task:
            val = msg.text.strip()
            task["add_text"] = "" if val == "-" else val
            save_task(task)
        user_states.pop(uid, None)
        await msg.reply("✅ Add-text updated.", reply_markup=kb_main())

    elif step == "delwords":
        task = get_task(state["tid"]); added = 0
        if task:
            for line in msg.text.split("\n"):
                word = line.strip()
                if word and word not in task["delete_words"]:
                    task["delete_words"].append(word); added += 1
            save_task(task)
        user_states.pop(uid, None)
        await msg.reply(
            f"✅ Added **{added}** delete rule(s). Total: **{len(task.get('delete_words', []))}**",
            reply_markup=kb_main()
        )

    elif step == "repwords":
        task = get_task(state["tid"]); added = 0
        if task:
            for line in msg.text.split("\n"):
                if "=>" in line:
                    old, new = line.split("=>", 1)
                    old = old.strip(); new = new.strip()
                    if old:
                        task["replace_words"][old] = new; added += 1
            save_task(task)
        user_states.pop(uid, None)
        await msg.reply(
            f"✅ Added **{added}** replacement rule(s). Total: **{len(task.get('replace_words', {}))}**",
            reply_markup=kb_main()
        )

# ─────────────────────────────────────────────
# FORWARD LOGIC
# ─────────────────────────────────────────────

async def run_task(client: Client, task: dict) -> tuple[int, str]:
    count = 0; err = ""
    source = task["source"]; dest = task["dest"]; last = task.get("last_id", 0)

    try:
        messages = []
        async for msg in client.get_chat_history(source, limit=200):
            if msg.id <= last:
                break
            messages.append(msg)

        if not messages:
            return 0, ""

        messages.reverse()  # oldest first

        for msg in messages:
            try:
                is_mp4 = bool(msg.video or (msg.document and msg.document.mime_type == "video/mp4"))
                is_pdf = bool(msg.document and msg.document.mime_type == "application/pdf")

                if is_mp4 or is_pdf:
                    new_cap = process_caption(msg.caption, task)
                    await client.copy_message(
                        chat_id=dest,
                        from_chat_id=source,
                        message_id=msg.id,
                        caption=new_cap or None,
                    )
                    count += 1
                    await asyncio.sleep(2)

                update_last_id(task["id"], msg.id)

            except FloodWait as fw:
                log.warning(f"FloodWait {fw.value}s – task {task['name']}")
                await asyncio.sleep(fw.value + 2)
            except Exception as e:
                err = str(e)
                log.error(f"Task {task['name']} msg-level error: {e}")

    except (ChannelPrivate, UsernameNotOccupied, ChatAdminRequired) as e:
        err = str(e)
    except Exception as e:
        err = str(e)
        log.error(f"Task {task['name']} fatal: {e}")

    return count, err

# ─────────────────────────────────────────────
# BACKGROUND WORKER
# ─────────────────────────────────────────────

INTERVAL = 30 * 60

async def auto_forward_worker():
    log.info("Background worker started — every 30 min")
    while True:
        tasks  = get_tasks()
        owners = get_owners()
        for tid, task in tasks.items():
            if not task.get("enabled", True):
                log.info(f"Skipping disabled task: {task['name']}")
                continue
            log.info(f"Running task: {task['name']}")
            count, err = await run_task(app, task)
            log.info(f"  → forwarded {count} message(s)")
            if err:
                for oid in owners:
                    try:
                        await app.send_message(
                            oid,
                            f"⚠️ **Error in task '{task['name']}'**\n"
                            f"Source: `{task['source']}`\n"
                            f"Error: `{err}`"
                        )
                    except Exception:
                        pass
        log.info("Cycle done. Sleeping 30 min.")
        await asyncio.sleep(INTERVAL)

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

async def main():
    await app.start()
    me = await app.get_me()
    log.info(f"Bot online as @{me.username}")
    asyncio.create_task(auto_forward_worker())
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
