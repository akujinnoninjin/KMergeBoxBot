import asyncio
import shutil
import discord
import os
from discord.ext import tasks, commands
from dotenv import load_dotenv
from datetime import datetime
import sqlite3 # oh no it has a database now
from cyptography.fernet import Fernet # and cryptography why

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

# HF upload mode env variables
enableHF=os.getenv('enableHF')
encryptionKey=os.getenv('encryptionKey')

# Job history mode env variables
enableHistory = os.getenv('enableHistory')

allowedCommands=os.getenv('allowedCommands').split(",") # Suggest: Regen, Status, Generate
privilegedCommands=os.getenv('privelegedCommands').split(",") # Suggest: HF Upload, HF Token
privilegedRoles=os.getenv('privilegedRoles').split(",")
adminCommands=os.getenv('adminCommands').split(",") # Suggest: Anything DB manipulatey
adminRoles=os.getenv('adminRoles').split(",")

# Sets base path (current directory)
os.chdir(basePath)

# Main bot class
class KMergeBoxBot(discord.Client):
    def __init__(self, command_prefix, intents, db_cursor):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.dbCursor = db_cursor
        
        #Encryption Setup
        self.cipherSuite = Fernet(encryptionKey)
        
    # List of current / ongoing tasks
    currentTasks = {}
    # List of current low priority / ongoing tasks
    currentLowPriorityTasks = {}
    # Current merge title and timestamp
    currentJob = ()

    # Start the merge watcher / runner in the background
    async def setup_hook(self) -> None:
        self.runMerges.start()

    # Log the logon event
    async def on_ready(self):
        print(f'Logged on as {self.user}!')
 
    # Is the user of an appropriate role for privileged commands
    ## APPLIES TO ALL COMMANDS
    @self.check
    async def user_has_persimmons(ctx):
        # If it's an admin command, check if they have the admin role
        if ctx.command.name in adminCommands:
            if any(role.id in ctx.author.roles for role in adminRoles)
                return True
            else return False
        # If it's a privileged command, check if they have privileged role
        if ctx.command.name in privilegedCommands:
            if any(role.id in ctx.author.roles for role in privilegedRoles)
                return True
            else return False
        # If it's an allowed command
        if ctx.command.name in allowedCommands:
            return True
        # If it didn't appear on any of the three lists, the command is disabled
        return False
        
    def is_message_for_me():
        async def predicate(ctx):
            # Only return true if command didn't come from the bot and is in correct channel
            return not ctx.author.id == ctx.bot.user.id and ctx.channel.id == channelToListenOn
        return commands.check(predicate)
        
    def user_has_no_existing_tasks():
        async def predicate(ctx):
            # Return true if user ID is not already in the task list
            if not ctx.author.id in self.currentTasks.keys() and not ctx.author.id in self.currentLowPriorityTasks.keys():
               return True
            # Otherwise notify and return false to abort the command
            await ctx.channel.send(f'{ctx.author.mention} has already submitted a pending task (please try and submit it again later): {self.currentTasks[ctx.author.id]}')   
            return False              
        return commands.check(predicate)
    
    def message_has_valid_yaml_attachment():
        async def predicate(ctx):
            # Message should only have one attachment, and it shoud be a yaml
            return len(ctx.message.attachments) == 1 and ctx.message.attachments[0].filename.lower().endswith(".yaml")
        return commands.check(predicate)    
        
    def is_hf_enabled():
        async def predicate(ctx):
            # Disable command if hf is not enabled
            return enableHF
        return commands.check(predicate)
        
    # !regen command
    @self.command()
    @is_message_for_me()
    @user_has_no_existing_tasks()
    async def regen(ctx, args):
        # Add the task to the queue
        self.currentTasks[ctx.author.id] = args[0]
        
        # Give feedback
        print(f'Rerunning {args[0]} submitted from {ctx.author}')
        await ctx.channel.send(f'Rerunning {args[0]} submitted from {ctx.author}')
        return
     
    # !generate command
    # Takes an attachment and an optional "update" flag to overwrite an existing yaml if the user owns it. 
    @self.command()
    @is_message_for_me()
    @user_has_no_existing_tasks()
    @message_has_valid_yaml_attachment()
    async def generate(ctx, args)
        attachment = ctx.message.attachments[0]
        locToSaveTo = path.join(basePath,attachment.filename)
    
        # If the named merge already has been run, respond with an error
        if path.exists(locToSaveTo):
            await ctx.channel.send(f'The file {attachment.filename} already has been merged before. Please choose a different name {ctx.author.mention}.')
            return
            
        # If the file contains gated words, then don't do the merge unless the user has the designated role and even then run at lower priority
        data = (await attachment.read()).decode("utf-8")
        isGated = any(word in data.lower() for word in gatedWords)
        if isGated and not any(role.id == gatedWordsRole for role in ctx.author.roles):
            await ctx.channel.send(gatedWordsError)
            return
            
        # If the file contains banned words, don't run it
        if any(word in data.lower() for word in forbiddenWords):
            await ctx.channel.send(f'The file {attachment.filename} contains forbidden words and cannot be run.')
            return

        # Save the attachment
        await attachment.save(locToSaveTo)
        
        # Add the job to the database for the user
        if enableHistory:
            self.dbCursor.execute('INSERT INTO job_history (user_id, job_name) VALUES (?, ?)', (ctx.author.id, attachment.filename.rsplit(".", 1)[0]))

        # Add merge job to queue and then respond to requester
        if isGated:
            self.currentLowPriorityTasks[ctx.author.id] = attachment.filename
        else:
            self.currentTasks[ctx.author.id] = attachment.filename

        print(f'Attachment submitted from {ctx.author}: {ctx.message.content} and saved to {locToSaveTo}')
        await ctx.channel.send(f'Task submitted for {ctx.author.mention}: {attachment.filename}')
        return
    
    # @self.command()
    # @is_message_for_me()
    # @is_hf_enabled()
    # async def hfupload(ctx, args)
        # return
    
    #!hftoken command
    #Takes one argument (the hf token), encrypts it, and adds it to the database. Only works over DMs.
    @self.command()
    @dm_only()
    @is_hf_enabled()
    async def hftoken(ctx, args)
        if len(args) != 1
            await ctx.author.send(f"Too many arguments - just the token, please.")
            return
        if args[0][:3] != 'hf_':
            await ctx.author.send(f"That doesn't look like a valid hf token, it should begin 'hf_'. Please confirm and try again.")
            return
        
        # Encrypt and store the hf token in the database, keyed to the user's ID; overwriting existing 
        encryptedToken = cipherSuite.encrypt(args[0].encode())
        self.dbCursor.execute('INSERT OR REPLACE INTO hf_tokens (user_id, hf_token) VALUES (?, ?)', (ctx.author.id, encryptedToken))
        
        # Notify user, with a sanity check that converts back the stored data to ensure it looks right. Then remove the value after 15 seconds.
        message = await ctx.author.send(f"HF token for <@{ctx.author.id}> updated to {cipherSuite.decrypt(encryptedToken).decode} and encrypted.")
        await asyncio.sleep(15)
        await message.edit(f"HF token for <@{ctx.author.id}> updated to <censored> and encrypted.")
        return
    # Handle someone trying to use the command in a channel anyway
    @hftoken.error
    async def hftoken_error(ctx, error):
        if isinstance(error, PrivateMessageOnly): 
            # If the bot can delete their message, do so
            if ctx.channel.permissions_for(ctx.me).manage_messages
                await ctx.message.delete()
                await message = ctx.channel.send(f"{ctx.author.mention} - that command only works in DMs for security reasons.")
                await message.delete(delay=15)
            # Otherwise ~~scold~~ alert the user
            else
                await ctx.channel.reply(f"That command only works in DMs for security reasons, you might want to delete that and generate a new token.")
                await message.delete(delay=15)
        
    @self.command()
    @is_message_for_me()
    async def status(ctx, args)
        if self.currentJob[0] is not None:
            await ctx.channel.send(f'Currently {self.currentJob[0]}, started at {self.currentJob[1]}')
        else:
            await ctx.channel.send(f'Currently idle...')
        return 
 
    @tasks.loop(seconds=10)
    async def run_jobs(self):
        # Don't run jobs while cleaning or merging
        if self.currentJob[0] is not None:
            return

        # If either queue has a job, queue it up
        if len(self.currentTasks.keys()) != 0 or len(self.currentLowPriorityTasks.keys()) != 0:
            asyncio.ensure_future(self.run_first_item_in_queue())
           
        return
    
    # Waits for the user to be logged on before starting the run merges task
    @runJobs.before_loop
    async def before_run_jobs(self):
        await self.wait_until_ready()
        

    async def cleanup_space(self):
        self.currentJob = ("cleaning", datetime.now().strftime("%m-%d %H:%M:%S")) 
        print(f'Now {self.currentJob[0]}, started at {self.currentJob[1]}')
        
        # Start the cleanup job, piping outputs ready to be collected once complete
        cleanupCommand = f'sh ./cleanup.sh'
        process = await asyncio.create_subprocess_shell(cleanupCommand, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE)
        # Wait for the cleanup to complete, retrieve the outputs
        stdout, stderr = await process.communicate()
        
        # Put the standard out and error into a single string, log to console
        resultText = f'STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}'
        print(resultText)
        
        # Wait 30 seconds, then end the cleaning state
        await asyncio.sleep(30)
        self.currentJob = ()
        return

    async def run_first_item_in_queue(self):
        # Get first task in the ordered dictionary
        firstTask = next(iter(self.currentTasks.items()), next(iter(self.currentLowPriorityTasks.items()), ""))
        
        # Failsafe if somehow got here with an empty task list
        if firstTask == "":
            self.currentJob = ()
            return
        else:
            nameWithoutExt = firstTask[1].rsplit(".", 1)[0]
            self.currentJob = ("running merge: " + nameWithoutExt, datetime.now().strftime("%m-%d %H:%M:%S"))
            print(f'Now {self.currentJob[0]}, started at {self.currentJob[1]}')
        
        # Begin the merge job, piping outputs ready to be collected once complete
        commandToRun = f'sh ./run.sh {nameWithoutExt}'
        process = await asyncio.create_subprocess_shell(commandToRun, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE)
        # Wait for the merge to complete (no logs will be printed yet)
        stdout, stderr = await process.communicate()
        
        # Put the standard out and error into a single string, log to console and file
        resultText = f'STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}'
        print(resultText)
        logfile = path.join(basePath, 'log.txt')
        with open(logfile, 'w') as fileToWrite:
            fileToWrite.write(resultText)
            
        # Notify user of end of job with logs as attachment
        await self.get_channel(channelToListenOn).send(f'<@{firstTask[0]}> - {nameWithoutExt} has finished', file=discord.File(logfile))
        
        # Clear from the pending task queue and end the job
        self.currentTasks.pop(firstTask[0], None)
        self.currentLowPriorityTasks.pop(firstTask[0], None)
        self.currentJob = ()
        print(f'Ending merge: {nameWithoutExt}')
        
        # Run cleanup if needed
        total, used, free = shutil.disk_usage(__file__)
        if (used / total > cleanupThreshold):
            asyncio.ensure_future(self.cleanup_space())
        return



# Make database connection if enabled
if enableHistory or enableHF:
    conn = sqlite3.connect('user_data.db')
    c = conn.cursor()

    # Create DB tables if enabled:
    if enableHF:
        c.execute('''CREATE TABLE IF NOT EXISTS hf_tokens
                     (user_id INT PRIMARY KEY,
                      hf_token TEXT)''')
    if enableHistory:
        c.execute('''CREATE TABLE IF NOT EXISTS job_history
                     (job_id INT PRIMARY KEY AUTOINCREMENT,
                      user_id INT,
                      job_name TEXT,
                      FOREIGN KEY(user_id) REFERENCES hf_tokens(user_id)
                      ON DELETE CASCADE)''')
    conn.commit()
else:
    c = None
    
# Runs the merge bot with the provided API key
intents = discord.Intents.default()
intents.message_content = True

bot = KMergeBoxBot(command_prefix='!', intents=intents, db_cursor=c)
bot.run(apiKey)
