from datetime import datetime
import os
import asyncio
import shutil
import discord
from discord.ext import tasks, commands
from dotenv import load_dotenv

# Read config from .env file
load_dotenv()
basePath=os.getenv('basePath')
channelToListenOn=int(os.getenv('channelToListenOn'))
gatedWordsRole=int(os.getenv('gatedWordsRole'))
gatedWordsError=os.getenv('gatedWordsError')
gatedWords=os.getenv('gatedWords').split(",")
forbiddenWords=os.getenv('forbiddenWords').split(",")
cleanupThreshold=float(os.getenv('cleanupThreshold'))
apiKey=os.getenv('apiKey')

allowedCommands=os.getenv('allowedCommands').split(",") # Suggest: Regen, Status, Generate
privilegedCommands=os.getenv('privelegedCommands').split(",") # Suggest: HF Upload, HF Token
privilegedRoles=[int(role) for role in os.getenv('privilegedRoles').split(",")]
adminCommands=os.getenv('adminCommands').split(",") # Suggest: Anything DB manipulatey
adminRoles=[int(role) for role in os.getenv('adminRoles').split(",")]

# HF upload mode env variables and import
enableHF=bool(os.getenv('enableHF'))
encryptionKey=os.getenv('encryptionKey')
if enableHF:
    from cryptography.fernet import Fernet # cryptography why

# Job history mode env variables and import
enableHistory = bool(os.getenv('enableHistory'))
if enableHistory or enableHF:
    import sqlite3 # oh no it has a database now

# Sets base path (current directorypi)
os.chdir(basePath)

# Main bot class
class KMergeBoxBot(commands.Cog):
    def __init__(self, bot, db_cursor):
        self.bot = bot
        self.db_cursor = db_cursor

        #Encryption Setup
        if enableHF:
            self.cipher_suite = Fernet(encryptionKey)

    # List of current / ongoing tasks
    current_tasks = {}
    # List of current low priority / ongoing tasks
    current_low_priority_tasks = {}
    # Current merge title and timestamp
    current_job = ()

    # Start the merge watcher / runner in the background
    async def setup_hook(self) -> None:
        self.bot.run_jobs.start()

    # Log the logon event
    async def on_ready(self):
        print(f'Logged on as {self.bot.user}!')

    # Is the user of an appropriate role for privileged commands
    ## APPLIES TO ALL COMMANDS
    @commands.check
    async def user_has_persimmons(self, ctx):
        # If it's an admin command, check if they have the admin role
        if ctx.command.name in adminCommands:
            return any(role.id in ctx.author.roles for role in adminRoles)
        # If it's a privileged command, check if they have privileged role
        if ctx.command.name in privilegedCommands:
            return any(role.id in ctx.author.roles for role in privilegedRoles)
        # If it's an allowed command, return true
        return ctx.command.name in allowedCommands

    # Is the command destined for the bot?
    def is_message_for_me(self):
        async def predicate(ctx):
            # Only return true if command didn't come from the bot and is in correct channel
            return not ctx.author.id == ctx.bot.user.id and ctx.channel.id == channelToListenOn
        return commands.check(predicate)

    # Does the user have existing tasks queued?
    def user_has_no_existing_tasks(self):
        async def predicate(ctx):
            # Return true if user ID is not already in the task list
            if (not ctx.author.id in self.current_tasks
                and not ctx.author.id in self.current_low_priority_tasks):
                return True
            # Otherwise notify and return false to abort the command
            await ctx.channel.send(f"""
                {ctx.author.mention} has already submitted a pending task
                (please try and submit it again later): {self.current_tasks[ctx.author.id]}
                """)
            return False
        return commands.check(predicate)

    # Does the message have only a single yaml attachment?
    def message_has_valid_yaml_attachment(self):
        async def predicate(ctx):
            # Message should only have one attachment, and it shoud be a yaml
            return (len(ctx.message.attachments) == 1
                    and ctx.message.attachments[0].filename.lower().endswith(".yaml"))
        return commands.check(predicate)

    # Is the HF module enabled?
    def is_hf_enabled(self):
        async def predicate():
            # Disable command if hf is not enabled
            return enableHF
        return commands.check(predicate)

    # !regen command
    @commands.command()
    @is_message_for_me()
    @user_has_no_existing_tasks()
    async def regen(self, ctx, args):
        # Add the task to the queue
        self.current_tasks[ctx.author.id] = args[0]

        # Give feedback
        print(f'Rerunning {args[0]} submitted from {ctx.author}')
        await ctx.channel.send(f'Rerunning {args[0]} submitted from {ctx.author}')
        return

    # Shared merge task creator, called by on_message and !generate
    async def make_a_merge_task(self, message):
        attachment = message.attachments[0]
        save_location = os.path.join(basePath,attachment.filename)
        # If the named merge already has been run, respond with an error
        if os.path.exists(save_location):
            await message.channel.send(f"""
                The file {attachment.filename} already has been merged before.
                Please choose a different name {message.author.mention}.
                """)
            return

        # If the file contains gated words, then don't do the merge
        # ...unless the user has the designated role, but then run at lower priority
        data = (await attachment.read()).decode("utf-8")
        is_gated = any(word in data.lower() for word in gatedWords)
        if is_gated and not any(role.id == gatedWordsRole for role in message.author.roles):
            await message.channel.send(gatedWordsError)
            return

        # If the file contains banned words, don't run it
        if any(word in data.lower() for word in forbiddenWords):
            await message.channel.send(
                f'The file {attachment.filename} contains forbidden words and cannot be run.')
            return

        # Save the attachment
        await attachment.save(save_location)

        # Add the job to the database for the user
        if enableHistory:
            self.db_cursor.execute('INSERT INTO job_history (user_id, job_name) VALUES (?, ?)',
                                  (message.author.id, attachment.filename.rsplit(".", 1)[0]))

        # Add merge job to queue and then respond to requester
        if is_gated:
            self.current_low_priority_tasks[message.author.id] = attachment.filename
        else:
            self.current_tasks[message.author.id] = attachment.filename

        print( f"""Attachment submitted from {message.author}:
                   {message.content} and saved to {save_location}""")
        await message.channel.send(
            f'Task submitted for {message.author.mention}: {attachment.filename}')
        return

    # !generate command
    # Takes an attachment.
    @commands.command()
    @is_message_for_me()
    @message_has_valid_yaml_attachment()
    @user_has_no_existing_tasks()
    async def generate(self, ctx):
        self.make_a_merge_task(ctx.message)

    # Automated gen on yaml attachment
    @commands.Cog.listener()
    async def on_message(self, message):
        if (message.channel.id == channelToListenOn
                and message.author.id != self.bot.user.id):
            if (message.attachments and len(message.attachments) == 1
                    and message.attachments[0].filename.lower().endswith(".yaml")):
                if (not message.author.id in self.current_tasks
                        and not message.author.id in self.current_low_priority_tasks):
                    self.make_a_merge_task(message)
                # Already doing something
            # Too many attachments
        # Not for me!

    # Shared huggingface uploader, so can pull silly tricks with reactions later
    async def huggingface_upload(self, message, job):
        if not enableHF:
            print(f'Error - Upload requested for {job}, but huggingface mode disabled.')
            return

        # Retrieve database info
        self.db_cursor.execute(
            "SELECT hf_name, hf_token FROM hf_tokens WHERE user_id = ?", (message.author.id))
        result = self.db_cursor.fetchone()
        if result:
            username, apikey = result
            apikey = self.cipher_suite.decrypt(apikey).decode
            print(username) # This is here to shut pylint up for now
            # Create a repo if it doesn't exist
                  # Upload the folder in the background
                    # Integrate with task manager?
                    # Create *separate* task manager?
                  # Close the connection
                  # Let user know upload is complete
        else:
            await message.channel.send(f"""
                No HF login info found for {message.author}:
                 please send the bot a DM using `!hflogin name access_token`.
                """)
            return

    @commands.command()
    @is_message_for_me()
    @is_hf_enabled()
    @user_has_no_existing_tasks()
    async def hfupload(self, ctx, args):
        if len(args==0):
            job = args[0]
            # Trim it, if they asked for the yaml
            if job.lower().endswith(".yaml"):
                job = job.rsplit(".", 1)[0]
            self.huggingface_upload(ctx.message, job)
            return
        # Malformed request
        return

    #!hflogin command
    # Takes two arguments (hf name, hf acces token), encrypts the token, stores both in database.
    # Only works over DMs. Would be obviated by /commands
    @commands.command()
    @commands.dm_only()
    @is_hf_enabled()
    async def hflogin(self, ctx, args):
        if len(args) != 2:
            await ctx.author.send("Arguments should be 'hf_name access_token'.")
            return
        # if not validHFRepo(args[0] )
            #await ctx.author.send(f"Invalid huggingface name.")
            # call a validation routine to make sure it fits the repo style rules
            # return
        if args[1][:3] != 'hf_':
            await ctx.author.send("""
                Invalid hf token. It should begin 'hf_'. Please confirm and try again.
                """)
            return

        # Encrypt and store the hf token in the database, keyed to user's ID; overwriting existing
        encrypted_token = self.cipher_suite.encrypt(args[0].encode())
        self.db_cursor.execute(
            """INSERT OR REPLACE INTO hf_tokens (user_id, hf_name, hf_token) VALUES (?, ?, ?)""",
            (ctx.author.id, args[0], encrypted_token))

        # Notify user, converting back the stored data as a quick sanity check
        message = await ctx.author.send(f"""
            HF settings for <@{ctx.author.id}> updated to {args[0]} :
            {self.cipher_suite.decrypt(encrypted_token).decode} and encrypted.
            """)

        # Then censor the token after 15 seconds.
        await asyncio.sleep(15)
        await message.edit(f"""
            HF token for <@{ctx.author.id}> updated to {args[0]} :
            <censored> and encrypted.
            """)
        return
    # Handle someone trying to use the command in a channel anyway
    @hflogin.error
    async def hflogin_error(self, ctx, error):
        if isinstance(error, commands.PrivateMessageOnly):
            # If the bot can delete their message, do so
            if ctx.channel.permissions_for(ctx.me).manage_messages:
                await ctx.message.delete()
                message = await ctx.channel.send(f"""
                    {ctx.author.mention} - that command only works in DMs for security reasons.
                    """)
                await message.delete(delay=15)
            # Otherwise ~~scold~~ alert the user
            else:
                await ctx.channel.reply("""
                That command only works in DMs for security reasons, 
                you might want to delete that and generate a new token.
                """)
                await message.delete(delay=15)

    @commands.command()
    @is_message_for_me()
    async def status(self, ctx):
        if self.current_job[0] is not None:
            await ctx.channel.send(f"""
                Currently {self.current_job[0]}, started at {self.current_job[1]}
                """)
        else:
            await ctx.channel.send('Currently idle...')
        return

    @tasks.loop(seconds=10)
    async def run_jobs(self):
        # Don't run jobs while cleaning or merging
        if self.current_job[0] is not None:
            return

        # If either queue has a job, queue it up
        if len(self.current_tasks) != 0 or len(self.current_low_priority_tasks) != 0:
            asyncio.ensure_future(self.run_first_item_in_queue())
        return

    # Waits for the user to be logged on before starting the run merges task
    @run_jobs.before_loop
    async def before_run_jobs(self):
        await self.bot.wait_until_ready()


    async def cleanup_space(self):
        self.current_job = ("cleaning", datetime.now().strftime("%m-%d %H:%M:%S"))
        print(f'Now {self.current_job[0]}, started at {self.current_job[1]}')

        # Start the cleanup job, piping outputs ready to be collected once complete
        cleanup_command = 'sh ./cleanup.sh'
        process = await asyncio.create_subprocess_shell(
                            cleanup_command,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE)

        # Wait for the cleanup to complete, retrieve the outputs
        stdout, stderr = await process.communicate()

        # Log the standard out and error to console
        print(f'STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}')

        # Wait 30 seconds, then end the cleaning state
        await asyncio.sleep(30)
        self.current_job = ()
        return

    async def run_first_item_in_queue(self):
        # Get first task in the ordered dictionary
        first_task = next(iter(self.current_tasks.items()),
                        next(iter(self.current_low_priority_tasks.items()),"")
                        )

        # Failsafe if somehow got here with an empty task list
        if first_task == "":
            self.current_job = ()
            return

        name_without_ext = first_task[1].rsplit(".", 1)[0]
        self.current_job = ("running merge: " + name_without_ext,
                            datetime.now().strftime("%m-%d %H:%M:%S"))
        print(f'Now {self.current_job[0]}, started at {self.current_job[1]}')

        # Begin the merge job, piping outputs ready to be collected once complete
        command_to_run = f'sh ./run.sh {name_without_ext}'
        process = await asyncio.create_subprocess_shell(
                        command_to_run,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE)

        # Wait for the merge to complete (no logs will be printed yet)
        stdout, stderr = await process.communicate()

        # Put the standard out and error into a single string, log to console and file
        result_text = f'STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}'
        print(result_text)
        logfile = os.path.join(basePath, 'log.txt')
        with open(logfile, 'w', encoding="utf-8") as file_to_write:
            file_to_write.write(result_text)

        # Notify user of end of job with logs as attachment
        await self.bot.get_channel(channelToListenOn).send(f"""
            <@{first_task[0]}> - {name_without_ext} has finished""",
            file=discord.File(logfile))

        # Clear from the pending task queue and end the job
        self.current_tasks.pop(first_task[0], None)
        self.current_low_priority_tasks.pop(first_task[0], None)
        self.current_job = ()
        print(f'Ending merge: {name_without_ext}')

        # Run cleanup if needed
        total, used = shutil.disk_usage(__file__)
        if used / total > cleanupThreshold:
            asyncio.ensure_future(self.cleanup_space())
        return

# Make database connection if enabled
if enableHistory or enableHF:
    conn = sqlite3.connect('user_data.db')
    cur = conn.cursor()

    # Create DB tables if enabled:
    if enableHF:
        cur.execute('''CREATE TABLE IF NOT EXISTS hf_tokens
                       (user_id INT PRIMARY KEY,
                        hf_name TEXT,
                        hf_token TEXT)''')
    if enableHistory:
        cur.execute('''CREATE TABLE IF NOT EXISTS job_history
                       (job_id INT PRIMARY KEY AUTOINCREMENT,
                       user_id INT,
                       job_name TEXT,
                       FOREIGN KEY(user_id) REFERENCES hf_tokens(user_id)
                       ON DELETE CASCADE)''')
    conn.commit()

# Runs the merge bot with the provided API key

intents = discord.Intents.default()
intents.message_content = True
mergebot = KMergeBoxBot(command_prefix="!", intents=intents, db_cursor=cur)
mergebot.bot.run(apiKey)
