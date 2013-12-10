#!/usr/bin/env python

import socket,multiprocessing
import subprocess
from optparse import OptionParser,OptionGroup
import time,random
import cPickle
import datetime

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

# TODO: Config files for more complex jobs
# TODO: Default logfiles for the server?
# TODO: Config files to specify defaults
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
                a dictionary of properties of the currently active job. Will
                initially be a empty. Yet, as soon as the worker is actually
                dealing with a job, it will write properties of the job into
                this list. Because the list is used for inter process
                communication, it should again be created using
                multiprocessing.manager.
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
                job = self.queue.pop(0)
                for key,item in job.iteritems():
                    self.active_job[key] = item
                self.active_job['start'] = str(datetime.datetime.now())

                print "Worker",self.name
                print "  Running process",job['id'],"with command:",job['command']

                logf = open(job.setdefault('logfile','/dev/null'),'w')
                exc = subprocess.Popen ( job['command'],
                        stderr=subprocess.STDOUT,
                        stdout=logf,
                        shell=True,
                        cwd=job.setdefault('working_dir','.')
                        ).wait()
                logf.close()

                print "Worker",self.name
                print "  Finished command",job['command'],"exit code",exc
            else:
                # There is no job in the queue, put empty marks into the job
                # description and wait a bit for the next check.
                for k in self.active_job.keys():
                    self.active_job.pop(k)
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
            self.active_jobs.append(self.manager.dict({}))
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
            pcljob = channel.recv ( 1024 )
            channel.send ( pcljob )
            job = cPickle.loads(pcljob)

            # Perform action
            success = True
            if self.verbosity>0:
                print "Performing action:",action
                print "job:",job

            # try:
            result = eval ( 'self.%s(%s)' % (action,job) )
            # except:
            #     success = False
            #     result = None

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

    def submit ( self, job ):
        """Submit a job

        :Parameters:
            job
                a dictionary describing the job to be submitted
        """
        if job['command'] is None:
            return "No command specified, no job committed"
        procname = str(self.procname)
        self.procname += 1
        job['id'] = procname
        self.queue.append ( job )
        return "Submitted command: %s with job id %s" % (job['command'],job['id'])

    def quit ( self, ignored=None ):
        """Quit the server"""
        if self.verbosity>1:
            print "Quitting"
        self.isrunning = False
        return "Stopping server"

    def ls ( self, ignored=None ):
        """List all jobs"""
        # First we list all the jobs that are waiting
        tab = "Waiting (id,command,logfile)\n"
        if len(self.queue):
            for job in self.queue:
                tab += "%(id)6s\t%(command)50s\t%(logfile)10s\n" % job
        else:
            tab += "  No waiting jobs\n"
        tab += "\n"

        # Now we list the jobs that are currently active
        tab += "Active (worker,id,command,logfile,starting time)\n"
        for w,job in zip(self.workers,self.active_jobs):
            if len(job.keys()):
                tab += w.name + "   id: %(id)6s\n  command: %(command)s\n  logfile: %(logfile)s\n  working directory: %(working_dir)s\n  started at: %(start)20s\n" % job
            else:
                tab += w.name + " idle\n"

        return tab

    def cancel ( self, job ):
        """Cancel a given job

        :Parameters:
            job
                id of the job to cancel
        """
        # First, try to find the job in the queue of waiting jobs
        for i in xrange(len(self.queue)):
            if job==self.queue[i]['id']:
                removed = self.queue.pop(i)
                status = "waiting"
                break
        else:
            # Is the job active?
            for i in xrange(len(self.active_jobs)):
                if job==self.active_jobs[i]['id']:
                    # Now this is a bit risky. We have to kill the worker and
                    # create a new one.
                    removed = self.active_jobs[i]
                    status = "active"
                    self.workers[i].terminate()
                    self.workers[i] = Worker(self.queue,self.active_jobs[i])
                    self.workers[i].start()
                    break
            else:
                return "Didn't find job: %(id)s" % (job,)
        return "Removed %s job %s" % (status,str(removed))

def assemble_job ( opts, args ):
    if len(args)>0:
        action = args[0]
    else:
        print "No action specified! Aborting."
        return None

    job = {}
    job['command'] = opts.command
    job['logfile'] = opts.logfile
    job['working_dir'] = opts.working_dir

    return action,job

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

    clientoptions.add_option ( '-c', '--command',
            action='store',
            dest='command',
            help='command to be submitted' )
    clientoptions.add_option ( '-l', '--logfile',
            action='store',
            dest='logfile',
            help='logfile for the process' )
    clientoptions.add_option ( '-d', '--working-directory',
            action='store',
            dest='working_dir',
            help='specify working directory for the command' )

    parser.add_option_group ( serveroptions )
    parser.add_option_group ( clientoptions )
    opts,args = parser.parse_args ()

    if opts.server:
        print "Running server"
        server = Server(opts.njobs,verbosity=opts.verbosity)
        server.run()

    else:
        action,job = assemble_job ( opts, args )
        # Clientmode
        # Connect to the server
        print "Running client"
        client = socket.socket ( socket.AF_INET, socket.SOCK_STREAM )
        client.connect ( ('localhost', 2727) )

        # Send what you want to do and wait for replies
        print "Sending action request"
        client.send ( action )
        understood = client.recv ( 1024 )
        print "Server understood request:",understood
        # Assemble job in dictionary, pickle, and send
        pcljob = cPickle.dumps(job)
        client.send ( pcljob )
        understood = client.recv ( 1024 )
        print "Server understood command",cPickle.loads(understood)

        # At this point, we should get a message about the status of the action
        print client.recv ( 1024 )

        client.shutdown(socket.SHUT_RDWR)
        client.close( )
