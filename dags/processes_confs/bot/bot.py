import logging

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Replace 'YOUR_BOT_TOKEN' with the token you get from BotFather on Telegram
TOKEN = "6477062934:AAGp0HLhzOwpJGSaJZ_Q2_bs-qPqRaoOuC8"


# Enable logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def send_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /chat_id is issued."""
    username = update.effective_user.full_name
    await update.message.reply_text(f"Olá! {username}\n\nO seu chat ID é:\n{update.message.chat_id}")
    logger.info("Sent chat id.")


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(TOKEN).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("chat_id", send_chat_id))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
