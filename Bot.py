import asyncio
import shutil
from os import path

import discord
import os
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
    # Is cleanup happening
    currentlyCleaning = False
    # Is a merge running
    currentlyMerging = False

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
        if self.currentlyCleaning:
            await ctx.channel.send(f'Currently running a cleanup job...')
        else if self.currentlyMerging:
            ## TODO: get current job info
            await ctx.channel.send(f'Currently running a merge...')
        else:
            await ctx.channel.send(f'Currently idle...')
        return 
    
 
    @tasks.loop(seconds=10)
    async def runMerges(self):
        # Check disk space free
        if self.currentlyCleaning == True:
            return
        total, used, free = shutil.disk_usage(__file__)
        if (used / total > cleanupThreshold):
            print("Running cleanup")
            asyncio.ensure_future(self.cleanupSpace())

        # If no jobs in queue, skip
        if len(self.currentTasks.keys()) == 0 and len(self.currentLowPriorityTasks.keys()) == 0:
            return
        if self.currentlyMerging == True:
            return
        self.currentlyMerging = True
        asyncio.ensure_future(self.runFirstItemInQueue())

    async def cleanupSpace(self):
        self.currentlyCleaning = True
        # Declare the merge job command
        commandToRun = f'sh ./cleanup.sh'
        # Start up the merge process, piping outputs ready to be collected once complete
        process = await asyncio.create_subprocess_shell(commandToRun, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE)
        # Wait for the merge to complete (no logs will be printed yet)
        stdout, stderr = await process.communicate()
        # Put the standard out and error into a single string
        resultText = f'STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}'
        # Print to logs (console and file)
        print(resultText)
        await asyncio.sleep(30)
        self.currentlyCleaning = False

    async def runFirstItemInQueue(self):
        # Get first task in the ordered dictionary
        firstTask = next(iter(self.currentTasks.items()), next(iter(self.currentLowPriorityTasks.items()), ""))
        if firstTask == "":
            self.currentlyMerging = False
            return
        attachment = firstTask[1]
        # Get the name without the extension
        nameWithoutExt = attachment.replace('.yaml', '')
        print(f'Starting merge: {nameWithoutExt}')
        # Declare the merge job command
        commandToRun = f'sh ./run.sh {nameWithoutExt}'
        # Start up the merge process, piping outputs ready to be collected once complete
        process = await asyncio.create_subprocess_shell(commandToRun, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        # Wait for the merge to complete (no logs will be printed yet)
        stdout, stderr = await process.communicate()
        # Put the standard out and error into a single string
        resultText = f'STDOUT: {stdout.decode()}, STDERR: {stderr.decode()}'
        # Print to logs (console and file)
        print(resultText)
        locToSaveTo = path.join(basePath, 'log.txt')
        with open(locToSaveTo, 'w') as fileToWrite:
            fileToWrite.write(resultText)
        # Get the channel to respond to
        channel = self.get_channel(channelToListenOn)
        # Respond with the logs as an attachment
        file = discord.File(locToSaveTo)
        await channel.send(f'<@{firstTask[0]}> - {nameWithoutExt} has finished', file=file)
        # Clear from the pending task queue
        task = self.currentTasks.get(firstTask[0], None)
        if task:
            del self.currentTasks[firstTask[0]]
        task = self.currentLowPriorityTasks.get(firstTask[0], None)
        if task:
            del self.currentLowPriorityTasks[firstTask[0]]
        self.currentlyMerging = False
        print(f'Ending merge: {nameWithoutExt}')

    # Waits for the user to be logged on before starting the run merges task
    @runMerges.before_loop
    async def before_my_task(self):
        await self.wait_until_ready()

# Runs the merge bot with the provided API key
intents = discord.Intents.default()
intents.message_content = True

bot = KMergeBoxBot(command_prefix='!', intents=intents)
bot.run(apiKey)
