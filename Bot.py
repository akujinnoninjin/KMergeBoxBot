import asyncio
import shutil
from os import path

import discord
import os
from discord.ext import tasks, commands
from dotenv import load_dotenv
from datetime import datetime

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

# Sets base path (current directory)
os.chdir(basePath)

# Main bot class
class KMergeBoxBot(discord.Client):
    def __init__(self, command_prefix, intents):
        super().__init__(command_prefix=command_prefix, intents=intents)
        
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
 
    def is_message_for_me():
        async def predicate(ctx):
            # Only return true if command didn't come from the bot and is in correct channel
            return not ctx.author.id == ctx.bot.user.id and ctx.channel.id == channelToListenOn
        return commands.check(predicate)
        
    def user_has_no_existing_tasks():
        async def predicate(ctx):
            # Return true if user ID is not already in the task list
            if not ctx.author.id in self.currentTasks.keys() and not ctx.author.id in self.currentLowPriorityTasks.keys():
               return true
            # Otherwise notify and return false to abort the command
            await ctx.channel.send(f'{ctx.author.mention} has already submitted a pending task (please try and submit it again later): {self.currentTasks[ctx.author.id]}')   
            return false              
        return commands.check(predicate)
    
    def message_has_valid_yaml_attachment():
        async def predicate(ctx):
            # Message should only have one attachment, and it shoud be a yaml
            return len(ctx.message.attachments) == 1 and ctx.message.attachments[0].filename.endswith(".yaml")
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
    # async def replace(ctx, args)
        # return
    
    # @self.command()
    # @is_message_for_me()
    # async def remove(ctx, args)
        # return
    
    # @self.command()
    # @is_message_for_me()
    # async def hf(ctx, args)
        # return
            
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
            nameWithoutExt = firstTask[1].replace('.yaml', '')
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

# Runs the merge bot with the provided API key
intents = discord.Intents.default()
intents.message_content = True

bot = KMergeBoxBot(command_prefix='!', intents=intents)
bot.run(apiKey)
