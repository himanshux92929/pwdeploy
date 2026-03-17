"""
Auto-Forwarder Telegram Bot
────────────────────────────
• Forwards MP4 + PDF from public source channels to destination channels
• Full management via inline keyboard (DM, owners only)
• All state persisted in Supabase (ephemeral-safe)
• Channel ID validation + public-channel verification on input
• Structured Caption Manager per task
• Background worker every 30 min; manual Run Now
• Owner DM error alerts
"""

import os, json, uuid, asyncio, logging, tempfile, traceback
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message,
)
from pyrogram.errors import (
    FloodWait, ChannelPrivate, UsernameNotOccupied,
    ChatAdminRequired, PeerIdInvalid, UsernameInvalid,
    ChatIdInvalid,
)
from supabase import create_client, Client as SupabaseClient

# ══════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# CONFIG  (environment variables)
# ══════════════════════════════════════════════
API_ID       = int(os.environ.get("API_ID", "0"))
API_HASH     = os.environ.get("API_HASH", "")
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")  # service-role key

DEFAULT_OWNER = 7380969878
INTERVAL      = 30 * 60   # seconds between auto-forward cycles

# ══════════════════════════════════════════════
# SUPABASE
# ══════════════════════════════════════════════
sb: SupabaseClient = create_client(SUPABASE_URL, SUPABASE_KEY)

# ──────────────────────────────────────────────
# DB helpers – owners
# ──────────────────────────────────────────────
def get_owners() -> list[int]:
    try:
        res = sb.table("owners").select("user_id").execute()
        ids = [r["user_id"] for r in res.data]
        return ids if ids else [DEFAULT_OWNER]
    except Exception as e:
        log.error(f"get_owners: {e}")
        return [DEFAULT_OWNER]

def add_owner(uid: int):
    sb.table("owners").upsert({"user_id": uid}).execute()

def remove_owner(uid: int):
    sb.table("owners").delete().eq("user_id", uid).execute()

# ──────────────────────────────────────────────
# DB helpers – tasks
# ──────────────────────────────────────────────
def _fix(row: dict) -> dict:
    row["delete_words"]  = row.get("delete_words")  or []
    row["replace_words"] = row.get("replace_words") or {}
    row["add_lines"]     = row.get("add_lines")     or []
    row.setdefault("keep_caption", True)
    row.setdefault("enabled",      True)
    row.setdefault("last_id",      0)
    row.setdefault("add_text",     "")
    return row

def get_tasks() -> dict:
    try:
        res = sb.table("tasks").select("*").execute()
        return {r["id"]: _fix(r) for r in res.data}
    except Exception as e:
        log.error(f"get_tasks: {e}"); return {}

def get_task(tid: str) -> dict | None:
    try:
        res = sb.table("tasks").select("*").eq("id", tid).single().execute()
        return _fix(res.data) if res.data else None
    except Exception as e:
        log.error(f"get_task({tid}): {e}"); return None

def save_task(t: dict):
    sb.table("tasks").upsert({
        "id": t["id"], "name": t["name"],
        "source": t["source"], "dest": t["dest"],
        "last_id": t.get("last_id", 0),
        "keep_caption":  t.get("keep_caption",  True),
        "delete_words":  t.get("delete_words",  []),
        "replace_words": t.get("replace_words", {}),
        "add_text":      t.get("add_text",      ""),
        "add_lines":     t.get("add_lines",     []),
        "enabled":       t.get("enabled",       True),
    }).execute()

def delete_task(tid: str):
    sb.table("tasks").delete().eq("id", tid).execute()

def update_last_id(tid: str, new_id: int):
    sb.table("tasks").update({"last_id": new_id}).eq("id", tid).execute()

def new_tid() -> str:
    return str(uuid.uuid4())[:8]

# ──────────────────────────────────────────────
# Export / import
# ──────────────────────────────────────────────
def export_json() -> str:
    return json.dumps({"owners": get_owners(), "tasks": get_tasks()},
                      indent=2, ensure_ascii=False)

def import_json(raw: str):
    data = json.loads(raw)
    sb.table("owners").delete().neq("user_id", -1).execute()
    for uid in data.get("owners", [DEFAULT_OWNER]):
        add_owner(uid)
    sb.table("tasks").delete().neq("id", "").execute()
    for t in data.get("tasks", {}).values():
        save_task(t)

# ══════════════════════════════════════════════
# CAPTION PROCESSOR
# ══════════════════════════════════════════════
def process_caption(original: str | None, task: dict) -> str | None:
    """
    Returns the processed caption string, or None to strip caption entirely.
    Pipeline: keep? → replace → delete → add_text → add_lines
    """
    if not task.get("keep_caption", True):
        # Caption OFF: only send add_text/add_lines if present
        extra = _build_extra(task)
        return extra if extra else None

    text = original or ""

    # 1. Replace words/phrases
    for old, new in (task.get("replace_words") or {}).items():
        text = text.replace(old, new)

    # 2. Delete words/phrases
    for word in (task.get("delete_words") or []):
        text = text.replace(word, "")

    # 3. Append add_text
    add = task.get("add_text", "").strip()
    if add:
        text = text.rstrip() + f"\n\n{add}"

    # 4. Append add_lines (each on its own line)
    for line in (task.get("add_lines") or []):
        text = text.rstrip() + f"\n{line}"

    result = text.strip()
    return result if result else None

def _build_extra(task: dict) -> str:
    parts = []
    if task.get("add_text", "").strip():
        parts.append(task["add_text"].strip())
    for line in (task.get("add_lines") or []):
        parts.append(line)
    return "\n".join(parts)

# ══════════════════════════════════════════════
# PYROGRAM CLIENT
# ══════════════════════════════════════════════
app = Client("forwarder_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_states: dict[int, dict] = {}

# ──────────────────────────────────────────────
# Owner filter
# ──────────────────────────────────────────────
async def _is_owner(_, __, update):
    uid = update.from_user.id if update.from_user else None
    return uid in get_owners()

owner_filter = filters.create(_is_owner)

# ══════════════════════════════════════════════
# CHANNEL VALIDATOR
# ══════════════════════════════════════════════
async def validate_public_channel(client: Client, identifier: str) -> tuple[bool, str, str]:
    """
    Returns (ok, resolved_id_str, error_message)
    Accepts @username or numeric ID (with or without -100 prefix).
    Verifies channel exists and is public (accessible without membership).
    """
    identifier = identifier.strip()
    try:
        chat = await client.get_chat(identifier)
    except (PeerIdInvalid, UsernameNotOccupied, UsernameInvalid, ChatIdInvalid):
        return False, "", "❌ Channel not found. Check the username or ID."
    except ChannelPrivate:
        return False, "", "❌ This channel is **private**. Only public channels are supported."
    except Exception as e:
        return False, "", f"❌ Could not resolve channel: `{e}`"

    # Must be a channel or supergroup
    if chat.type.value not in ("channel", "supergroup"):
        return False, "", "❌ That is not a channel. Please send a channel ID or username."

    # Public check: has a username
    if not chat.username:
        return False, "", "❌ This channel is **private** (no public username). Only public channels supported."

    # Normalise to -100XXXXXXX format
    cid = str(chat.id)
    return True, cid, ""

# ══════════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════════
def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Manage Tasks",  callback_data="view_tasks"),
         InlineKeyboardButton("➕ New Task",       callback_data="add_task")],
        [InlineKeyboardButton("👥 Owners",         callback_data="manage_owners"),
         InlineKeyboardButton("📤 Export JSON",    callback_data="export_json")],
        [InlineKeyboardButton("📥 Import JSON",    callback_data="import_json")],
    ])

def kb_task_list(tasks: dict):
    rows = []
    for tid, t in tasks.items():
        icon = "🟢" if t.get("enabled", True) else "🔴"
        rows.append([InlineKeyboardButton(f"{icon} {t['name']}  [{t['source']}]",
                                          callback_data=f"task_{tid}")])
    rows.append([InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)

def kb_task(t: dict):
    tid = t["id"]
    on  = t.get("enabled", True)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'⏸ Pause' if on else '▶️ Resume'} Task",
                              callback_data=f"toggle_{tid}"),
         InlineKeyboardButton("🗑 Delete Task",      callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("✏️ Rename",            callback_data=f"rename_{tid}"),
         InlineKeyboardButton("🔄 Change Source",     callback_data=f"chsrc_{tid}")],
        [InlineKeyboardButton("📬 Change Dest",       callback_data=f"chdst_{tid}"),
         InlineKeyboardButton("🔢 Set Last Msg ID",   callback_data=f"setlid_{tid}")],
        [InlineKeyboardButton("📝 Caption Manager",   callback_data=f"capmgr_{tid}"),
         InlineKeyboardButton("▶️ Run Now",            callback_data=f"runnow_{tid}")],
        [InlineKeyboardButton("🔙 Task List",          callback_data="view_tasks")],
    ])

def kb_caption(t: dict):
    tid = t["id"]
    cap = t.get("keep_caption", True)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📋 Caption: {'✅ ON' if cap else '❌ OFF'}  (tap to toggle)",
                              callback_data=f"togglecap_{tid}")],
        [InlineKeyboardButton("🗑 Add Delete Filter",  callback_data=f"delwords_{tid}"),
         InlineKeyboardButton("📋 View Delete Filters",callback_data=f"viewdel_{tid}")],
        [InlineKeyboardButton("🔁 Add Replace Rule",   callback_data=f"repwords_{tid}"),
         InlineKeyboardButton("📋 View Replace Rules", callback_data=f"viewrep_{tid}")],
        [InlineKeyboardButton("➕ Add Append Text",    callback_data=f"addtext_{tid}"),
         InlineKeyboardButton("📋 View Append Text",   callback_data=f"viewappend_{tid}")],
        [InlineKeyboardButton("🗑 Clear All Filters",  callback_data=f"clearall_{tid}"),
         InlineKeyboardButton("🔙 Back to Task",       callback_data=f"task_{tid}")],
    ])

def kb_back_task(tid: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"task_{tid}")]])

def kb_back_cap(tid: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Caption Manager", callback_data=f"capmgr_{tid}")]])

# ══════════════════════════════════════════════
# TASK INFO TEXT
# ══════════════════════════════════════════════
def task_info(t: dict) -> str:
    on  = "🟢 Running" if t.get("enabled", True) else "🔴 Paused"
    cap = "✅ On" if t.get("keep_caption", True) else "❌ Off"
    dw  = len(t.get("delete_words", []))
    rw  = len(t.get("replace_words", {}))
    at  = bool(t.get("add_text") or t.get("add_lines"))
    return (
        f"🔧 **Task: {t['name']}**\n\n"
        f"  Status   : {on}\n"
        f"  Source   : `{t['source']}`\n"
        f"  Dest     : `{t['dest']}`\n"
        f"  Last ID  : `{t['last_id']}`\n"
        f"  Caption  : {cap}\n"
        f"  Delete filters : {dw}\n"
        f"  Replace rules  : {rw}\n"
        f"  Append text    : {'Yes' if at else 'No'}"
    )

# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════
def _reroute(cq: CallbackQuery, new_data: str) -> CallbackQuery:
    cq.data = new_data
    return cq

async def notify_owners(text: str):
    for oid in get_owners():
        try:
            await app.send_message(oid, text)
        except Exception:
            pass

# ══════════════════════════════════════════════
# /start  /cancel
# ══════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private & owner_filter)
async def cmd_start(_, msg: Message):
    user_states.pop(msg.from_user.id, None)
    await msg.reply(
        "👋 **Auto-Forwarder Bot**\n\n"
        "Forwards MP4 videos and PDFs from public source channels to destination channels.\n\n"
        "Choose an option:",
        reply_markup=kb_main()
    )

@app.on_message(filters.command("cancel") & filters.private & owner_filter)
async def cmd_cancel(_, msg: Message):
    user_states.pop(msg.from_user.id, None)
    await msg.reply("❌ Cancelled.", reply_markup=kb_main())

# ══════════════════════════════════════════════
# CALLBACK QUERY ROUTER
# ══════════════════════════════════════════════
@app.on_callback_query(owner_filter)
async def cb(client: Client, cq: CallbackQuery):
    d   = cq.data
    uid = cq.from_user.id

    # ── Main menu ────────────────────────────────────────────────────────────
    if d == "main_menu":
        user_states.pop(uid, None)
        await cq.message.edit_text("👋 **Auto-Forwarder Bot** – Main Menu:", reply_markup=kb_main())

    # ── Task list ────────────────────────────────────────────────────────────
    elif d == "view_tasks":
        tasks = get_tasks()
        if not tasks:
            await cq.answer("No tasks yet!", show_alert=True); return
        await cq.message.edit_text("📋 **All Tasks** – tap to manage:", reply_markup=kb_task_list(tasks))

    # ── Task detail ──────────────────────────────────────────────────────────
    elif d.startswith("task_"):
        tid = d[5:]; t = get_task(tid)
        if not t: await cq.answer("Not found!", show_alert=True); return
        await cq.message.edit_text(task_info(t), reply_markup=kb_task(t))

    # ── Toggle enabled ───────────────────────────────────────────────────────
    elif d.startswith("toggle_"):
        tid = d[7:]; t = get_task(tid)
        if t:
            t["enabled"] = not t.get("enabled", True); save_task(t)
            await cq.answer(f"Task {'resumed ▶️' if t['enabled'] else 'paused ⏸'}!")
            await cb(client, _reroute(cq, f"task_{tid}"))

    # ── Delete task ──────────────────────────────────────────────────────────
    elif d.startswith("del_task_"):
        tid = d[9:]
        await cq.message.edit_text(
            "⚠️ **Delete this task permanently?**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, delete", callback_data=f"confirmdelete_{tid}"),
                 InlineKeyboardButton("❌ Cancel",       callback_data=f"task_{tid}")]
            ])
        )

    elif d.startswith("confirmdelete_"):
        tid = d[14:]; delete_task(tid); await cq.answer("Deleted!")
        tasks = get_tasks()
        if tasks:
            await cq.message.edit_text("📋 **Tasks:**", reply_markup=kb_task_list(tasks))
        else:
            await cq.message.edit_text("No tasks remaining.", reply_markup=kb_main())

    # ── Run now ──────────────────────────────────────────────────────────────
    elif d.startswith("runnow_"):
        tid = d[7:]; t = get_task(tid)
        if not t: await cq.answer("Not found!", show_alert=True); return
        await cq.answer("▶️ Starting…")
        await cq.message.edit_text(f"⏳ Running **{t['name']}**…")
        count, err = await run_task(client, t)
        result = f"✅ **{t['name']}** done — forwarded **{count}** file(s)."
        if err:
            result += f"\n\n⚠️ Error:\n`{err}`"
        await cq.message.edit_text(result, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Task", callback_data=f"task_{tid}")]
        ]))

    # ── Caption Manager ──────────────────────────────────────────────────────
    elif d.startswith("capmgr_"):
        tid = d[7:]; t = get_task(tid)
        if not t: await cq.answer("Not found!", show_alert=True); return
        cap = t.get("keep_caption", True)
        text = (
            f"📝 **Caption Manager – {t['name']}**\n\n"
            f"Caption forwarding is currently **{'ON ✅' if cap else 'OFF ❌'}**\n\n"
            f"Delete filters : {len(t.get('delete_words', []))} rule(s)\n"
            f"Replace rules  : {len(t.get('replace_words', {}))} rule(s)\n"
            f"Append text    : {'Set' if t.get('add_text') else 'None'}\n"
            f"Append lines   : {len(t.get('add_lines', []))} line(s)"
        )
        await cq.message.edit_text(text, reply_markup=kb_caption(t))

    # ── Toggle caption ───────────────────────────────────────────────────────
    elif d.startswith("togglecap_"):
        tid = d[10:]; t = get_task(tid)
        if t:
            t["keep_caption"] = not t.get("keep_caption", True); save_task(t)
            await cq.answer(f"Caption {'ON ✅' if t['keep_caption'] else 'OFF ❌'}!")
            await cb(client, _reroute(cq, f"capmgr_{tid}"))

    # ── View delete filters ──────────────────────────────────────────────────
    elif d.startswith("viewdel_"):
        tid = d[8:]; t = get_task(tid)
        if not t: return
        dw = t.get("delete_words", [])
        if not dw:
            text = "🗑 **Delete Filters**\n\n_No delete filters set._"
        else:
            lines = "\n".join(f"`{i+1}.` `{w}`" for i, w in enumerate(dw))
            text  = f"🗑 **Delete Filters** ({len(dw)} rule(s)):\n\n{lines}"
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add More",           callback_data=f"delwords_{tid}"),
             InlineKeyboardButton("🗑 Clear All Deletes",  callback_data=f"cleardel_{tid}")],
            [InlineKeyboardButton("🔙 Back",               callback_data=f"capmgr_{tid}")]
        ]))

    elif d.startswith("cleardel_"):
        tid = d[9:]; t = get_task(tid)
        if t:
            t["delete_words"] = []; save_task(t); await cq.answer("Cleared!")
            await cb(client, _reroute(cq, f"viewdel_{tid}"))

    # ── View replace rules ───────────────────────────────────────────────────
    elif d.startswith("viewrep_"):
        tid = d[8:]; t = get_task(tid)
        if not t: return
        rw = t.get("replace_words", {})
        if not rw:
            text = "🔁 **Replace Rules**\n\n_No replace rules set._"
        else:
            lines = "\n".join(f"`{i+1}.` `{k}` → `{v or '(delete)'}`"
                              for i, (k, v) in enumerate(rw.items()))
            text = f"🔁 **Replace Rules** ({len(rw)} rule(s)):\n\n{lines}"
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add More",            callback_data=f"repwords_{tid}"),
             InlineKeyboardButton("🗑 Clear All Replaces",  callback_data=f"clearrep_{tid}")],
            [InlineKeyboardButton("🔙 Back",                callback_data=f"capmgr_{tid}")]
        ]))

    elif d.startswith("clearrep_"):
        tid = d[9:]; t = get_task(tid)
        if t:
            t["replace_words"] = {}; save_task(t); await cq.answer("Cleared!")
            await cb(client, _reroute(cq, f"viewrep_{tid}"))

    # ── View append text ─────────────────────────────────────────────────────
    elif d.startswith("viewappend_"):
        tid = d[11:]; t = get_task(tid)
        if not t: return
        at = t.get("add_text", "").strip()
        al = t.get("add_lines", [])
        text = "➕ **Append Settings**\n\n"
        text += f"**Add text block:**\n`{at}`\n\n" if at else "**Add text block:** _none_\n\n"
        if al:
            text += "**Append lines:**\n" + "\n".join(f"• `{l}`" for l in al)
        else:
            text += "**Append lines:** _none_"
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Set Text Block",     callback_data=f"addtext_{tid}"),
             InlineKeyboardButton("➕ Add Line",            callback_data=f"addline_{tid}")],
            [InlineKeyboardButton("🗑 Clear Text Block",   callback_data=f"cleartext_{tid}"),
             InlineKeyboardButton("🗑 Clear Lines",        callback_data=f"clearlines_{tid}")],
            [InlineKeyboardButton("🔙 Back",               callback_data=f"capmgr_{tid}")]
        ]))

    elif d.startswith("cleartext_"):
        tid = d[10:]; t = get_task(tid)
        if t:
            t["add_text"] = ""; save_task(t); await cq.answer("Text block cleared!")
            await cb(client, _reroute(cq, f"viewappend_{tid}"))

    elif d.startswith("clearlines_"):
        tid = d[11:]; t = get_task(tid)
        if t:
            t["add_lines"] = []; save_task(t); await cq.answer("Lines cleared!")
            await cb(client, _reroute(cq, f"viewappend_{tid}"))

    elif d.startswith("clearall_"):
        tid = d[9:]; t = get_task(tid)
        if t:
            t["delete_words"] = []; t["replace_words"] = {}
            t["add_text"] = ""; t["add_lines"] = []
            save_task(t); await cq.answer("All filters cleared!")
            await cb(client, _reroute(cq, f"capmgr_{tid}"))

    # ── Manage owners ────────────────────────────────────────────────────────
    elif d == "manage_owners":
        owners = get_owners()
        text   = "👥 **Owners:**\n" + "\n".join(f"• `{o}`" for o in owners)
        await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Owner",    callback_data="addowner"),
             InlineKeyboardButton("🗑 Remove Owner", callback_data="removeowner")],
            [InlineKeyboardButton("🔙 Main Menu",    callback_data="main_menu")]
        ]))

    # ── Export ───────────────────────────────────────────────────────────────
    elif d == "export_json":
        await cq.answer("Generating…")
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w",
                                         encoding="utf-8", delete=False) as f:
            f.write(export_json()); tmp = f.name
        await client.send_document(uid, tmp, caption="📦 **Supabase snapshot**")
        Path(tmp).unlink(missing_ok=True)

    # ── Import ───────────────────────────────────────────────────────────────
    elif d == "import_json":
        user_states[uid] = {"step": "import_json"}
        await cq.message.edit_text(
            "📥 Send a **data.json** file to overwrite Supabase.\n\n/cancel to abort.")

    # ── Text-input launchers ─────────────────────────────────────────────────
    elif d == "add_task":
        user_states[uid] = {"step": "task_name"}
        await cq.message.edit_text(
            "📝 **New Task – 1/3**\n\nSend the **task name** (e.g. `MovieSync`).\n\n/cancel to abort.")

    elif d == "addowner":
        user_states[uid] = {"step": "add_owner"}
        await cq.message.edit_text("👤 Send the **User ID** to add as owner.\n\n/cancel to abort.")

    elif d == "removeowner":
        user_states[uid] = {"step": "remove_owner"}
        await cq.message.edit_text("👤 Send the **User ID** to remove.\n\n/cancel to abort.")

    elif d.startswith("rename_"):
        tid = d[7:]; user_states[uid] = {"step": "rename", "tid": tid}
        await cq.message.edit_text("✏️ Send the **new name** for this task.\n\n/cancel to abort.")

    elif d.startswith("chsrc_"):
        tid = d[6:]; user_states[uid] = {"step": "chsrc", "tid": tid}
        await cq.message.edit_text(
            "📡 Send the **new source channel**.\n"
            "Accepted: `@username` or numeric ID (e.g. `-1001234567890`)\n\n"
            "_Only public channels are supported._\n\n/cancel to abort.")

    elif d.startswith("chdst_"):
        tid = d[6:]; user_states[uid] = {"step": "chdst", "tid": tid}
        await cq.message.edit_text(
            "📬 Send the **new destination channel**.\n"
            "Accepted: `@username` or numeric ID\n\n"
            "_Bot must be admin in the destination channel._\n\n/cancel to abort.")

    elif d.startswith("setlid_"):
        tid = d[7:]; user_states[uid] = {"step": "setlid", "tid": tid}
        t   = get_task(tid)
        await cq.message.edit_text(
            f"🔢 **Set Last Message ID**\n\nCurrent value: `{t['last_id'] if t else 0}`\n\n"
            "• Send `0` to re-forward **everything** from the beginning.\n"
            "• Send a message ID to start forwarding from **after** that ID.\n\n/cancel to abort.")

    elif d.startswith("delwords_"):
        tid = d[9:]; user_states[uid] = {"step": "delwords", "tid": tid}
        await cq.message.edit_text(
            "🗑 **Add Delete Filters**\n\n"
            "Send words or phrases to **remove** from captions.\n"
            "• One per line — multi-line phrases are supported.\n"
            "• They are **added** to the existing list.\n\n"
            "Example:\n`@spamchannel\nBuy now!\nhttp://bad.link`\n\n/cancel to abort.")

    elif d.startswith("repwords_"):
        tid = d[9:]; user_states[uid] = {"step": "repwords", "tid": tid}
        await cq.message.edit_text(
            "🔁 **Add Replace Rules**\n\n"
            "Format: `old text => new text` — one rule per line.\n"
            "• Leave right side empty to simply delete.\n\n"
            "Examples:\n"
            "`@oldhandle => @newhandle`\n"
            "`BadWord => `\n"
            "`Join us at => Follow us at`\n\n/cancel to abort.")

    elif d.startswith("addtext_"):
        tid = d[8:]; user_states[uid] = {"step": "addtext", "tid": tid}
        await cq.message.edit_text(
            "➕ **Set Append Text Block**\n\n"
            "This text is added as a **paragraph** at the bottom of every caption.\n"
            "Send `-` to clear it.\n\n/cancel to abort.")

    elif d.startswith("addline_"):
        tid = d[8:]; user_states[uid] = {"step": "addline", "tid": tid}
        await cq.message.edit_text(
            "➕ **Add Append Lines**\n\n"
            "Send lines to append at the bottom of every caption.\n"
            "One line per message. They are **added** to the existing list.\n\n/cancel to abort.")

# ══════════════════════════════════════════════
# TEXT MESSAGE HANDLER  (state machine)
# ══════════════════════════════════════════════
@app.on_message(filters.private & owner_filter & ~filters.command(["start", "cancel"]))
async def handle_text(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = user_states.get(uid)

    if not state:
        await msg.reply("Use /start to open the Control Panel.")
        return

    step = state.get("step")
    text = (msg.text or msg.caption or "").strip() if not msg.document else ""

    # ── Import JSON ──────────────────────────────────────────────────────────
    if step == "import_json":
        if msg.document and msg.document.file_name.endswith(".json"):
            path = await msg.download()
            try:
                import_json(Path(path).read_text(encoding="utf-8"))
                user_states.pop(uid, None)
                await msg.reply("✅ Imported into Supabase!", reply_markup=kb_main())
            except Exception as e:
                await msg.reply(f"❌ Failed: `{e}`", reply_markup=kb_main())
            finally:
                Path(path).unlink(missing_ok=True)
        else:
            await msg.reply("Please send a **.json** file.")
        return

    # ── Add / remove owner ───────────────────────────────────────────────────
    if step == "add_owner":
        try:
            add_owner(int(text)); user_states.pop(uid, None)
            await msg.reply(f"✅ Owner `{text}` added.", reply_markup=kb_main())
        except ValueError:
            await msg.reply("❌ Send a numeric User ID.")
        return

    if step == "remove_owner":
        try:
            remove_owner(int(text)); user_states.pop(uid, None)
            await msg.reply(f"✅ Owner `{text}` removed.", reply_markup=kb_main())
        except ValueError:
            await msg.reply("❌ Send a numeric User ID.")
        return

    # ── New task wizard ──────────────────────────────────────────────────────
    if step == "task_name":
        if not text:
            await msg.reply("Please send a text name."); return
        state["task_name"] = text; state["step"] = "task_source"
        await msg.reply(
            f"✅ Name: **{text}**\n\n"
            "**Step 2/3** – Send the **Source Channel**\n"
            "Accepted: `@username` or numeric channel ID\n\n"
            "_Only public channels supported._\n\n/cancel to abort.")
        return

    if step == "task_source":
        wait = await msg.reply("🔍 Verifying channel…")
        ok, resolved, err = await validate_public_channel(client, text)
        if not ok:
            await wait.edit_text(err + "\n\nPlease send a valid **public channel** username or ID.")
            return
        state["source"] = resolved; state["step"] = "task_dest"
        await wait.edit_text(
            f"✅ Source verified: `{resolved}`\n\n"
            "**Step 3/3** – Send the **Destination Channel**\n"
            "Accepted: `@username` or numeric ID\n\n"
            "_Bot must be admin in destination._\n\n/cancel to abort.")
        return

    if step == "task_dest":
        wait = await msg.reply("🔍 Checking destination…")
        # Destination just needs to resolve; bot may not be admin yet
        dest_in = text.strip()
        try:
            chat = await client.get_chat(dest_in)
            resolved_dest = str(chat.id)
        except Exception as e:
            await wait.edit_text(
                f"❌ Could not resolve destination: `{e}`\n\nPlease try again.")
            return
        tid  = new_tid()
        task = {
            "id": tid, "name": state["task_name"],
            "source": state["source"], "dest": resolved_dest,
            "last_id": 0, "keep_caption": True,
            "delete_words": [], "replace_words": {},
            "add_text": "", "add_lines": [], "enabled": True,
        }
        save_task(task)
        user_states.pop(uid, None)
        await wait.edit_text(
            f"✅ **Task `{task['name']}` created!**\n\n"
            f"ID       : `{tid}`\n"
            f"Source   : `{task['source']}`\n"
            f"Dest     : `{resolved_dest}`\n"
            f"Caption  : On ✅ (default)\n"
            f"Status   : Running 🟢\n\n"
            "Use **Caption Manager** to set up filters.",
            reply_markup=kb_main()
        )
        return

    # ── Edit fields ──────────────────────────────────────────────────────────
    if step == "rename":
        t = get_task(state["tid"])
        if t:
            t["name"] = text; save_task(t)
        user_states.pop(uid, None)
        await msg.reply(f"✅ Task renamed to **{text}**.", reply_markup=kb_main())
        return

    if step == "chsrc":
        wait = await msg.reply("🔍 Verifying new source…")
        ok, resolved, err = await validate_public_channel(client, text)
        if not ok:
            await wait.edit_text(err + "\n\nSend a valid public channel or /cancel.")
            return
        t = get_task(state["tid"])
        if t:
            t["source"] = resolved; save_task(t)
        user_states.pop(uid, None)
        await wait.edit_text(f"✅ Source updated to `{resolved}`.", reply_markup=kb_main())
        return

    if step == "chdst":
        try:
            chat = await client.get_chat(text.strip())
            resolved_dest = str(chat.id)
        except Exception as e:
            await msg.reply(f"❌ Could not resolve: `{e}`\n\nTry again or /cancel.")
            return
        t = get_task(state["tid"])
        if t:
            t["dest"] = resolved_dest; save_task(t)
        user_states.pop(uid, None)
        await msg.reply(f"✅ Destination updated to `{resolved_dest}`.", reply_markup=kb_main())
        return

    if step == "setlid":
        try:
            new_id = int(text)
            update_last_id(state["tid"], new_id)
            user_states.pop(uid, None)
            await msg.reply(
                f"✅ Last ID set to `{new_id}`.\n"
                f"{'⚠️ Set to 0 — bot will re-forward ALL history.' if new_id == 0 else ''}",
                reply_markup=kb_main()
            )
        except ValueError:
            await msg.reply("❌ Send a valid integer, or /cancel.")
        return

    if step == "addtext":
        t = get_task(state["tid"])
        if t:
            t["add_text"] = "" if text == "-" else text
            save_task(t)
        user_states.pop(uid, None)
        await msg.reply(
            "✅ Append text block cleared." if text == "-" else "✅ Append text block updated.",
            reply_markup=kb_main()
        )
        return

    if step == "addline":
        t = get_task(state["tid"]); added = 0
        if t:
            for line in msg.text.split("\n"):
                l = line.strip()
                if l and l not in t["add_lines"]:
                    t["add_lines"].append(l); added += 1
            save_task(t)
        user_states.pop(uid, None)
        await msg.reply(
            f"✅ Added **{added}** append line(s). Total: **{len(t.get('add_lines', []))}**",
            reply_markup=kb_main()
        )
        return

    if step == "delwords":
        t = get_task(state["tid"]); added = 0
        if t:
            for line in msg.text.split("\n"):
                w = line.strip()
                if w and w not in t["delete_words"]:
                    t["delete_words"].append(w); added += 1
            save_task(t)
        user_states.pop(uid, None)
        await msg.reply(
            f"✅ Added **{added}** delete filter(s). Total: **{len(t.get('delete_words', []))}**",
            reply_markup=kb_main()
        )
        return

    if step == "repwords":
        t = get_task(state["tid"]); added = 0; bad = []
        if t:
            for line in msg.text.split("\n"):
                line = line.strip()
                if not line: continue
                if "=>" in line:
                    old, new = line.split("=>", 1)
                    old = old.strip(); new = new.strip()
                    if old:
                        t["replace_words"][old] = new; added += 1
                else:
                    bad.append(line)
            save_task(t)
        user_states.pop(uid, None)
        reply = f"✅ Added **{added}** replace rule(s). Total: **{len(t.get('replace_words', {}))}**"
        if bad:
            reply += f"\n\n⚠️ Skipped (no `=>`):\n" + "\n".join(f"• `{b}`" for b in bad)
        await msg.reply(reply, reply_markup=kb_main())
        return

# ══════════════════════════════════════════════
# FORWARD LOGIC
# ══════════════════════════════════════════════
async def run_task(client: Client, task: dict) -> tuple[int, str]:
    """
    Forward all new MP4/PDF messages from source to dest.
    Uses copy_message which works on both restricted and unrestricted public channels.
    Returns (count_forwarded, last_error_string).
    """
    count = 0
    last_err = ""
    source = task["source"]
    dest   = task["dest"]
    last   = int(task.get("last_id", 0))

    try:
        # Collect messages newer than last_id (get_chat_history = newest first)
        batch: list = []
        async for msg in client.get_chat_history(source, limit=300):
            if msg.id <= last:
                break
            batch.append(msg)

        if not batch:
            return 0, ""

        batch.reverse()  # process oldest → newest

        for msg in batch:
            try:
                is_video = bool(
                    msg.video or
                    (msg.document and msg.document.mime_type in
                     ("video/mp4", "video/x-matroska", "video/quicktime"))
                )
                is_pdf = bool(
                    msg.document and
                    msg.document.mime_type == "application/pdf"
                )

                if is_video or is_pdf:
                    new_cap = process_caption(msg.caption, task)
                    # copy_message handles both protected and unprotected channels
                    await client.copy_message(
                        chat_id=dest,
                        from_chat_id=source,
                        message_id=msg.id,
                        caption=new_cap,
                    )
                    count += 1
                    await asyncio.sleep(2)   # flood control

                # Always advance last_id even for non-matching messages
                update_last_id(task["id"], msg.id)

            except FloodWait as fw:
                log.warning(f"[{task['name']}] FloodWait {fw.value}s")
                await asyncio.sleep(fw.value + 3)
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                log.error(f"[{task['name']}] msg-level error: {last_err}")

    except (ChannelPrivate, ChatIdInvalid, UsernameNotOccupied,
            UsernameInvalid, PeerIdInvalid) as e:
        last_err = f"Channel access error: {e}"
    except ChatAdminRequired as e:
        last_err = f"Bot is not admin in destination: {e}"
    except Exception as e:
        last_err = f"{type(e).__name__}: {e}"
        log.error(f"[{task['name']}] fatal: {last_err}\n{traceback.format_exc()}")

    return count, last_err

# ══════════════════════════════════════════════
# BACKGROUND WORKER
# ══════════════════════════════════════════════
async def auto_forward_worker():
    log.info("⏱ Background worker started — 30 min interval")
    while True:
        tasks  = get_tasks()
        owners = get_owners()
        log.info(f"▶️ Cycle start — {len(tasks)} task(s)")

        for tid, task in tasks.items():
            if not task.get("enabled", True):
                continue
            log.info(f"  → {task['name']}")
            try:
                count, err = await run_task(app, task)
                log.info(f"     forwarded {count} file(s)")
                if err:
                    await notify_owners(
                        f"⚠️ **Error in task '{task['name']}'**\n\n"
                        f"Source : `{task['source']}`\n"
                        f"Dest   : `{task['dest']}`\n"
                        f"Error  : `{err}`"
                    )
            except Exception as e:
                msg = f"💥 **Crash in task '{task['name']}'**\n`{e}`"
                log.error(msg)
                await notify_owners(msg)

        log.info("✅ Cycle done — sleeping 30 min")
        await asyncio.sleep(INTERVAL)

# ══════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════
async def main():
    await app.start()
    me = await app.get_me()
    log.info(f"🤖 Bot online: @{me.username}")
    asyncio.create_task(auto_forward_worker())
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
