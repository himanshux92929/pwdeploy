import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from supabase import create_client, Client as SupabaseClient

# --- CONFIGURATION ---
API_ID = "YOUR_API_ID"
API_HASH = "YOUR_API_HASH"
BOT_TOKEN = "YOUR_BOT_TOKEN"

SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_SERVICE_ROLE_KEY" # Use service role key to bypass RLS

# Initialize Supabase and Pyrogram
supabase: SupabaseClient = create_client(SUPABASE_URL, SUPABASE_KEY)
app = Client("advanced_forwarder", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Dictionary to track user conversation states (for the UI)
user_states = {}

# --- DATABASE HELPERS ---
def get_owners():
    res = supabase.table("owners").select("user_id").execute()
    return [row["user_id"] for row in res.data]

def get_tasks():
    res = supabase.table("tasks").select("*").execute()
    return res.data

def update_last_id(task_id, new_id):
    supabase.table("tasks").update({"last_id": new_id}).eq("id", task_id).execute()

# --- CAPTION PROCESSOR ---
def process_caption(caption: str, keep: bool, delete_list: list, replace_dict: dict, add_text: str) -> str:
    # If caption is disabled, just return the added text (if any)
    if not keep:
        return add_text if add_text else ""
    
    text = caption or ""
    
    # 1. Replace specific words/phrases first
    for old, new in replace_dict.items():
        text = text.replace(old, new)
    
    # 2. Delete unwanted words/lines (including multi-line strings)
    for word in delete_list:
        text = text.replace(word, "")
        
    # 3. Add extra text to the bottom
    if add_text:
        text += f"\n\n{add_text}"
        
    return text.strip()

# --- FILTER FOR OWNERS ONLY ---
async def is_owner(_, __, message):
    user_id = message.from_user.id if isinstance(message, Message) else message.from_user.id
    return user_id in get_owners()

owner_only = filters.create(is_owner)

# --- BACKGROUND WORKER (Runs every 30 mins) ---
async def auto_forward_worker():
    await app.start()
    print("Background worker started...")
    
    while True:
        tasks = get_tasks()
        owners = get_owners()
        
        for task in tasks:
            try:
                # Fetch history chronologically starting after last_id
                async for msg in app.get_chat_history(task["source"], offset_id=task["last_id"], reverse=True):
                    if msg.id <= task["last_id"]:
                        continue
                        
                    is_mp4 = msg.video
                    is_pdf = msg.document and msg.document.mime_type == "application/pdf"
                    
                    if is_mp4 or is_pdf:
                        new_caption = process_caption(
                            msg.caption, 
                            task["keep_caption"], 
                            task.get("delete_words", []), 
                            task.get("replace_words", {}),
                            task.get("add_text", "")
                        )
                        
                        await app.copy_message(
                            chat_id=task["dest"],
                            from_chat_id=task["source"],
                            message_id=msg.id,
                            caption=new_caption
                        )
                        await asyncio.sleep(2) # Flood control
                    
                    update_last_id(task["id"], msg.id)
                    
            except Exception as e:
                # Notify owners
                error_msg = f"⚠️ **Error in Task '{task['name']}'**\nSource: {task['source']}\nError: `{str(e)}`"
                for owner_id in owners:
                    try:
                        await app.send_message(owner_id, error_msg)
                    except:
                        pass
                        
        print("Cycle complete. Sleeping for 30 minutes...")
        await asyncio.sleep(30 * 60)

# --- INTERACTIVE BUTTON UI ---

def get_main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 View Tasks", callback_data="view_tasks")],
        [InlineKeyboardButton("➕ Add New Task", callback_data="add_task")],
        [InlineKeyboardButton("👥 Manage Owners", callback_data="manage_owners")]
    ])

@app.on_message(filters.command("start") & filters.private & owner_only)
async def start_cmd(client, message):
    await message.reply(
        "👋 **Welcome to the Auto-Forwarder Control Panel.**\n\nWhat would you like to do?",
        reply_markup=get_main_menu()
    )

@app.on_callback_query(owner_only)
async def handle_callbacks(client, callback_query: CallbackQuery):
    data = callback_query.data
    
    if data == "main_menu":
        await callback_query.message.edit_text("Main Menu:", reply_markup=get_main_menu())
        
    elif data == "view_tasks":
        tasks = get_tasks()
        if not tasks:
            await callback_query.answer("No tasks found!", show_alert=True)
            return
            
        # Create a button for each task
        buttons = []
        for task in tasks:
            buttons.append([InlineKeyboardButton(f"🔧 {task['name']}", callback_data=f"edit_task_{task['id']}")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        
        await callback_query.message.edit_text("Select a task to manage:", reply_markup=InlineKeyboardMarkup(buttons))
        
    elif data == "add_task":
        # Enter state machine to wait for user input
        user_states[callback_query.from_user.id] = "waiting_for_task_name"
        await callback_query.message.edit_text(
            "📝 **Send me the name of the new task:**\n*(e.g., MovieSync)*\n\nSend /cancel to abort."
        )

# --- STATE MACHINE HANDLER (For capturing text inputs) ---
@app.on_message(filters.private & owner_only & filters.text)
async def handle_state_input(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    
    if message.text == "/cancel":
        if user_id in user_states:
            del user_states[user_id]
        await message.reply("❌ Action canceled.", reply_markup=get_main_menu())
        return

    # Example of handling the first step of adding a task
    if state == "waiting_for_task_name":
        task_name = message.text
        # Save temp data and move to next state
        user_states[user_id] = {"state": "waiting_for_source", "task_name": task_name}
        await message.reply(f"Task name set to `{task_name}`.\n\nNow send me the **Source Channel ID or Username**:")
        
    elif isinstance(state, dict) and state.get("state") == "waiting_for_source":
        source = message.text
        state["source"] = source
        state["state"] = "waiting_for_dest"
        await message.reply(f"Source set to `{source}`.\n\nNow send me the **Destination Channel ID or Username**:")
        
    elif isinstance(state, dict) and state.get("state") == "waiting_for_dest":
        dest = message.text
        # Save to Supabase
        supabase.table("tasks").insert({
            "name": state["task_name"],
            "source": state["source"],
            "dest": dest
        }).execute()
        
        del user_states[user_id]
        await message.reply("✅ **Task successfully added to database!**", reply_markup=get_main_menu())

# --- RUN THE BOT ---
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(auto_forward_worker())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
