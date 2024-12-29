import csv
import logging
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, Message
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pyrogram.errors import FloodWait
import asyncio
import os


API_ID = 26864378
API_HASH = "91ad28a746afb436c6fc2fd025aaef86"
BOT_TOKEN = "7539399059:AAFeEBRHYssjJyeqv4Kq8n8WOxMkjyL_khM"  # Replace with your Bot Token
MONGO_URI = "mongodb+srv://shadowprotocol144:EXqLCnwMUT6sdgM4@cluster0.n2nlw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
  # Replace with your MongoDB connection string


app = Client("my_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


client = MongoClient(MONGO_URI)
db = client["telegram_bot"]
users_collection = db["users"]


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__nmessageame__)

@app.on_message(filters.command(["start"]))
async def start(client, ):
    user_id = message.from_user.id


    if not users_collection.find_one({"user_id": user_id}):
   
        users_collection.insert_one({"user_id": user_id})

  
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Open Web App", web_app=WebAppInfo(url="https://codex-ml.github.io/vehicle/"))]
    ])


    sent_message = await client.send_video(
        chat_id=user_id,
        video="http://codex-ml.tech/videos/vehi.mp4",
        caption=(
            "Welcome to the bot! Click the button below to find vehicle info in the web app.\n\n"
            "**How to use this BOT**\n"
            "**इस रोबोट का उपयोग कैसे करें**"
        ),
        reply_markup=keyboard
    )

    await asyncio.sleep(10)

  
    restart_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Restart", callback_data="restart")]
    ])
    await client.edit_message_reply_markup(
        chat_id=sent_message.chat.id,
        message_id=sent_message.id,
        reply_markup=restart_keyboard
    )

@app.on_callback_query(filters.regex("restart"))
async def restart_callback(client, callback_query):
    await callback_query.answer("Restarting...")
    await callback_query.message.reply_text("Click /start to begin again.")

@app.on_message(filters.command(["stats"]))
async def stats(client, message):
    total_users = users_collection.count_documents({})
    await message.reply_text(f"Total number of users: {total_users}")

@app.on_message(filters.command(["check"]))
async def check_duplicates(client, message: Message):
    # Find duplicate user IDs
    duplicates = users_collection.aggregate([
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ])

    duplicates_to_remove = []
    for duplicate in duplicates:
        user_id = duplicate["_id"]
        entries = list(users_collection.find({"user_id": user_id}))
        if len(entries) > 1:
            duplicates_to_remove.extend(entry["_id"] for entry in entries[1:])

    if duplicates_to_remove:
        users_collection.delete_many({"_id": {"$in": duplicates_to_remove}})
        await message.reply_text(f"Removed {len(duplicates_to_remove)} duplicate user entries.")
    else:
        await message.reply_text("No duplicate user entries found.")

@app.on_message(filters.command(["broadcast"]))
async def broadcast(client, message: Message):
    if not message.text.startswith("/broadcast "):
        return

    broadcast_message = message.text[len("/broadcast "):].strip()
    users = users_collection.find()
    total_sent, total_failed = 0, 0

    for user in users:
        try:
            await client.send_message(user["user_id"], broadcast_message)
            total_sent += 1
        except Exception as e:
            total_failed += 1

    await message.reply_text(
        f"Broadcast completed!\nTotal Sent: {total_sent}\nTotal Failed: {total_failed}"
    )


from time import sleep
from pymongo.errors import PyMongoError
from pyrogram.errors import FloodWait

@app.on_message(filters.document)
async def handle_csv(client, message: Message):
    if not (message.document.mime_type == 'text/csv' or message.document.file_name.endswith(('.csv', '.csn'))):
        await message.reply_text("Please send a valid CSV or CSN file.")
        return

    try:
   
        file_path = await message.download()
        logger.info(f"File downloaded to {file_path}")

        valid_users = []
        invalid_users = 0

  
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                for user_id in row:
                    try:
                        user_id = int(user_id.strip())  # Validate the user_id is an integer

                        # Handle database insertion with flood protection
                        try:
                            if not users_collection.find_one({"user_id": user_id}):
                                users_collection.insert_one({"user_id": user_id})
                                valid_users.append(user_id)
                            else:
                                logger.info(f"User ID {user_id} already exists in the database.")
                        except PyMongoError as db_error:
                            logger.error(f"Database error for user ID {user_id}: {db_error}")
                            sleep(1)  # Add a small delay before retrying
                            continue
                    except ValueError:
                        logger.error(f"Invalid user ID found: {user_id}")
                        invalid_users += 1  # Increment the invalid user count

   
        os.remove(file_path)
        logger.info(f"File {file_path} processed and deleted.")

  
        try:
            await message.reply_text(
                f"Processed file.\n"
                f"✅ Valid users added: {len(valid_users)}\n"
                f"❌ Invalid user IDs: {invalid_users}\n"
                f"ℹ️ Total entries processed: {len(valid_users) + invalid_users}"
            )
        except FloodWait as e:
            logger.warning(f"FloodWait: Sleeping for {e.value} seconds.")
            sleep(e.value)
            await message.reply_text(
                f"Processed file.\n"
                f"✅ Valid users added: {len(valid_users)}\n"
                f"❌ Invalid user IDs: {invalid_users}\n"
                f"ℹ️ Total entries processed: {len(valid_users) + invalid_users}"
            )

        logger.info(f"Successfully added {len(valid_users)} users to the database.")
    except Exception as e:
        # General error handling
        await message.reply_text(f"Error processing the file: {str(e)}")
        logger.error(f"Error processing the file: {str(e)}")


if __name__ == "__main__":
    app.run()
