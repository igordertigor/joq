#!/usr/bin/env python

import socket,multiprocessing
import subprocess
from optparse import OptionParser,OptionGroup
import time,random

usage = """joq [options] action [command [logfile]]

job manager for simple job queueing

The following four actions are defined:

    submit <command> [logfile]
        run command and send output to logfile. If no logfile is given,
        output is send to /dev/null
    quit
        quit the server and all running jobs
    ls
        list all waiting and running jobs (three columns: id,command,logfile)
    cancel <id>
        cancel jobs by job id"""

description = """This program manages multiple jobs that can be submitted asynchronously. It allows you to specify the number of jobs that can run in parallel, and any job that is submitted to the system will be dealed with as soon as one of the working processes is free.
"""

# TODO: Can we control the verbosity level?
# TODO: Default logfiles for the server?
# TODO: Implement more complex jobs as dictionaries
# TODO: Config files for more complex jobs
# TODO: Config files to specify defaults
# TODO: Option for working directory
# TODO: Reprioritise jobs (move them up in the queue)
# TODO: For running jobs: Starting time.
# TODO: Change the number of processes for a running server

class Worker (multiprocessing.Process):
    def __init__ ( self, queue, active_job ):
        """An isolated process that works one job after the other

        :Parameters:
            queue
                a list of tasks to work on. This list has be creating using
                mulitprocessing.manager so that tasks can be appended to the
                list from the main program.
            active_job
                a list of properties of the currently active job. Will
                initially be an empty list of three items. Yet, as soon as
                the worker is actually dealing with a job, it will write
                properties of the job into this list. Because the list is
                used for inter process communication, it should again be
                created using multiprocessing.manager.
        """
        self.queue = queue
        self.active_job = active_job
        multiprocessing.Process.__init__(self)
        print "Created worker",self.name
    def run ( self ):
        """Run the worker to infinity"""
        while True:
            if len(self.queue):
                # There is a job in the queue
                name,command,logfile = self.queue.pop(0)
                self.active_job[0] = name
                self.active_job[1] = command
                self.active_job[2] = logfile

                print "Worker",self.name
                print "  Running process",name,"with command:",command

                logf = open(logfile,'w')
                exc = subprocess.call ( command,
                        stderr=subprocess.STDOUT,
                        stdout=logf,
                        shell=True )
                logf.close()

                print "Worker",self.name
                print "Finished command",command,"exit code",exc
            else:
                # There is no job in the queue, put empty marks into the job
                # description and wait a bit for the next check.
                self.active_job[0] = ""
                self.active_job[1] = ""
                self.active_job[2] = ""
                time.sleep(2+random.random()) # Avoid exactly synchronous Workers

class Server ( object ):
    def __init__ ( self, njobs, verbosity=2 ):
        """The server instance

        :Parameters:
            njobs
                number of parallel jobs to run.

        :Note:
            There is currently no upper list on the number of jobs that can
            be in the queue!
        """
        self.verbosity = verbosity
        # Set up the connectivity
        if self.verbosity>1:
            print "Starting server"
        self.server = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        self.server.bind ( ('',2727) )
        self.server.listen ( 5 )

        # Initialize process organization
        if self.verbosity>1:
            print "Starting workers"
        self.procname = 1
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.list([])
        self.active_jobs = []
        self.workers     = []
        for i in xrange(njobs):
            self.active_jobs.append(self.manager.list(["","",""]))
            self.workers.append(Worker(self.queue,self.active_jobs[-1]))
            if self.verbosity>1:
                print "  ",self.workers[-1].name

    def run ( self ):
        """Run until quit is called"""
        self.isrunning = True

        # Start all the workers
        for w in self.workers:
            w.start()

        while self.isrunning:
            # Wait for client
            channel,details = self.server.accept()
            # Get information
            if self.verbosity>1:
                print "Waiting for action"
            action = channel.recv ( 1024 )
            channel.send ( action )
            if self.verbosity>1:
                print "Waiting for command"
            command = channel.recv ( 1024 )
            channel.send ( command )
            if self.verbosity>1:
                print "Waiting for logfile"
            logfile = channel.recv ( 1024 )

            # Perform action
            success = True
            if self.verbosity>0:
                print "Performing action:",action
                print "Using command:",command
                print "Using logfile:",logfile

            try:
                result = eval ( 'self.%s("%s","%s")' % (
                    action,command,logfile) )
            except:
                success = False
                result = None

            # Send status back to client
            channel.send ( "Action '%s' was %s\n%s" % (
                        action,
                        'successful' if success else 'not successful',
                        result) )

        # When quit was called, kill all the workers...
        if self.verbosity>1:
            print "Terminating workers"
        for w in self.workers:
            print "  ",w.name
            w.terminate()

        # ... and shutdown the server
        if self.verbosity>1:
            print "Shutting down server"
        self.server.shutdown(socket.SHUT_RDWR)
        self.server.close()

    def submit ( self, command, logfile ):
        """Submit a job

        :Parameters:
            command
                a string with the command to be executed
            logfile
                where should standard error and standard output of the job go?
        """
        procname = str(self.procname)
        self.procname += 1
        self.queue.append ( (procname,command,logfile) )
        return "Submitted command: %s in process %s" % (command,procname)

    def quit ( self, q='ignored', l='ignored' ):
        """Quit the server

        Both parameters are meaningless
        """
        if self.verbosity>1:
            print "Quitting"
        self.isrunning = False
        return "Stopping server"

    def ls ( self, q='ignored', l='ignored' ):
        """List all jobs

        Both parameters are meaningless
        """
        # First we list all the jobs that are waiting
        tab = "Waiting (id,command,logfile)\n"
        if len(self.queue):
            for task in self.queue:
                tab += "%6s\t%50s\t%10s\n" % task
        else:
            tab += "  No active jobs\n"
        tab += "\n"

        # Now we list the jobs that are currently active
        tab += "Active (worker,id,command,logfile)\n"
        for w,j in zip(self.workers,self.active_jobs):
            tab += w.name + " %6s\t%50s\t%10s\n" % tuple(j)

        return tab

    def cancel ( self, job, l='ignored' ):
        """Cancel a given job

        :Parameters:
            job
                id of the job to cancel
        """
        # First, try to find the job in the queue of waiting jobs
        for i in xrange(len(self.queue)):
            if job==self.queue[i][0]:
                removed = self.queue.pop(i)
                status = "waiting"
                break
        else:
            # Is the job active?
            for i in xrange(len(self.active_jobs)):
                if job==self.active_jobs[i][0]:
                    # Now this is a bit risky. We have to kill the worker and
                    # create a new one.
                    removed = tuple(self.active_jobs[i])
                    status = "active"
                    self.workers[i].terminate()
                    self.workers[i] = Worker(self.queue,self.active_jobs[i])
                    self.workers[i].start()
                    break
            else:
                return "Didn't find job: %s" % (job,)
        return "Removed %s job %s" % (status,str(removed))

if __name__ == "__main__":
    parser = OptionParser(usage=usage,description=description)
    serveroptions = OptionGroup ( parser, "Server Options",
            "These are meaningful if the programm runs as a server" )
    clientoptions = OptionGroup ( parser, "Client Options",
            "These are meaningful if you run the program as a client" )

    serveroptions.add_option ( '-s', '--server',
            action='store_true',
            help='run in server mode' )
    serveroptions.add_option ( '-n', '--njobs',
            action='store',
            type='int',
            dest='njobs',
            default=2,
            help='number of jobs to run in parallel' )
    serveroptions.add_option ( '-v','--verbosity',
            action='store',
            type='int',
            dest='verbosity',
            default=2,
            help='verbosity level (0: no messages at all, 1: only few and (supposedly) important messages, 2: tell me about everything.' )

    clientoptions.add_option ( '-a', '--action',
            action='store',
            help='action to perform' )

    parser.add_option_group ( serveroptions )
    parser.add_option_group ( clientoptions )
    opts,args = parser.parse_args ()

    if opts.server:
        print "Running server"
        server = Server(opts.njobs,verbosity=opts.verbosity)
        server.run()

    else:
        # Clientmode
        # Connect to the server
        print "Running client"
        client = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        client.connect ( ('localhost', 2727) )

        # Send what you want to do and wait for replies
        print "Sending action request"
        client.send ( opts.action )
        understood = client.recv ( 1024 )
        print "Server understood request:",understood
        if len(args)>0:
            print "Sending command:",args[0]
            client.send ( args[0] )
        else:
            print "Sending empty command"
            client.send ( "." )
        understood = client.recv ( 1024 )
        print "Server understood command",understood
        if len(args) > 1:
            print "Sending logfile",args[1]
            client.send ( args[1] )
        else:
            print "Using no logfile"
            client.send ( '/dev/null' )

        # At this point, we should get a message about the status of the action
        print client.recv ( 1024 )

        client.shutdown(socket.SHUT_RDWR)
        client.close( )
