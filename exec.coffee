#!/usr/bin/env coffee
# -*- compile-command: "./test.sh"; -*-
fs = require 'fs'
path = require 'path'
{exec} = require 'child_process'
rimraf = require 'rimraf'
mkdirp = require 'mkdirp'
async = require 'async'
kill = require 'tree-kill'
M = (s) -> console.log(s); s


merge = (obj, args...) ->
  merge_ = (A, B) ->
    return A if not B?
    (A[k] = v) for k, v of B; A
  merge_ obj, arg for arg in args; obj

numberToDigits = (number, ndigits) ->
  ('0'.repeat(ndigits) + number).slice(-ndigits)


filenameAndTimestamp = (absFilename, cb) ->
  fs.stat absFilename, (err, stat) ->
    if err
      throw "could notstat #{absFilename}: #{err}"
    cb null,
      filename: path.basename absFilename
      timestamp: stat.mtimeMs

dir2Array = (dirname, cb) ->
  fs.readdir dirname, (err, filenames) ->
    if err
      throw "could not read dir: #{err}"
    absFilenames = filenames.map (fn) -> "#{dirname}/#{fn}"
    async.map absFilenames, filenameAndTimestamp, (err, fnTs) ->
      if err
        throw "could not stat #{dirname}: #{JSON.stringify err}"
      fnTs = fnTs.sort (a, b) -> a.timestamp - b.timestamp
      cb fnTs.map (t) -> t.filename

updateArray = (array, dirname, cb) ->
  dir2Array dirname, (newArray) ->
    array.splice 0, array.length
    array.splice newArray.length, 0, newArray...
    cb()

mirrorArrayFromDir = (array, dirname, onchange) ->
  updateArray array, dirname, onchange
  fs.watch dirname, {}, (eventType, filename) ->
    return if eventType isnt 'change'
    updateArray array, dirname, onchange



class Queue
  constructor: (dirname, exec) ->
    @queueDir = dirname
    @exec = exec
    @lastChange = undefined
    @queue = []
    @inProgress = {}

    mirrorArrayFromDir @queue, @queueDir, () =>
      @lastChange = process.hrtime.bigint()

    interval = 100
    handleWorkQueue = () =>
      bail = () ->
        setTimeout handleWorkQueue, interval

      return bail() if @queue.length is 0
      return bail() if process.hrtime.bigint() - @lastChange < 1e9 / 2

      idleExecId = exec.getIdleExecutorId()
      return bail() if not idleExecId?

      jobName = (@queue.splice 0, 1)[0]  # find non blocked
      return bail() if not jobName?

      jobFilename = "#{@queueDir}/#{jobName}"

      fs.readFile jobFilename, (err, cmd) =>
        cmd = cmd.toString().trim()
        fs.unlink jobFilename, (err) =>
          @inProgress[jobName] = true
          exec.execute idleExecId, jobName, cmd, (code) =>
            delete @inProgress[jobName]
            M "[Q#{idleExecId}] job \"#{jobName}\" exit #{code}"

      return bail()

    setTimeout handleWorkQueue, 100



class Executor
  constructor: (shell, id, workdir) ->
    @shell = shell
    @id = id
    @status = 'idle'  # or 'pending' or 'busy'
    @workdir = workdir
    @child = undefined

  execute: (jobname, jobno, cmd, logfd, cb) =>
    @status = 'busy'

    cmd = "#{@shell} \"#{cmd}\""
    @child = exec cmd,
      cwd: @workdir
      env: merge process.env, { JOBNO: jobno }
    logfd.write "#{@shell} \"#{cmd}\"\n"

    @child.on 'error', (err) ->
      M "failed to exec: #{cmd}"
      M err
      process.exit 1

    @child.stdout.on 'data', (d) =>
      process.stdout.write "[child#{@id}] (stdout) #{d.toString()}"
      logfd.write d

    @child.stderr.on 'data', (d) =>
      process.stderr.write "[child#{@id}] (stderr) #{d.toString()}"
      logfd.write d

    @child.on 'exit', (code) =>
      code = 'cancelled' if code is null
      @child.stdout.end(); @child.stderr.end()
      logfd.write "\n#{code}\n"
      logfd.end()
      @status = 'idle'
      cb code

  cancel: (cb) =>
    return if @status isnt 'busy'
    kill @child.pid, 'SIGKILL', (err) =>
      if err
        return M "[child#{@id}] could not kill tree #{@child.pid}"



class Exec
  constructor: ({execdir, workdir, shells}) ->
    @workdir = workdir
    @rundir = "#{execdir}/run"
    @logdir = "#{execdir}/log"
    @queuedir = "#{execdir}/queue"
    @shells = shells
    @executors = []
    @queue = null

  start: (cb) =>

    clearRunDir = (_cb) =>
      rimraf @rundir, (err) =>
        return cb err if err
        _cb false

    startExecutors = (_cb) =>
      id = 0
      start = (shell, _cb) =>
        _id = id++
        @executors[_id] = new Executor shell, _id, @workdir
        mkdirp "#{@rundir}/#{_id}", _cb
      async.map @shells, start, _cb

    startQueue = (_cb) =>
      @queue = new Queue @queuedir, @
      _cb()

    clearRunDir -> startExecutors -> startQueue -> cb

  getIdleExecutorId: () =>
    idle = @executors.find (e) -> e.status is 'idle'
    if idle?
      idle.status = 'pending'
      idle.id

  execute: (id, jobname, cmd, cb) =>

    jobnameToLastJobno = (jobname, cb) =>
      pat = new RegExp "^#{jobname}\.([0-9]+)$"
      lognames = dir2Array @logdir, (lognames) ->
        matches = lognames.map (name) -> name.match pat
        matches = matches.filter (match) -> match
        jobnos = (matches.map (match) -> match[1]).sort()
        cb jobnos[jobnos.length - 1]

    jobnameToNextJobno = (jobname, cb) =>
      lastJobno = jobnameToLastJobno jobname, (lastJobno) =>
        return cb 0 if not lastJobno
        cb parseInt(lastJobno) + 1

    jobnameToLogfileStream = (jobname, cb) =>
      jobnameToNextJobno jobname, (nextJobno) =>
        jobno = numberToDigits nextJobno, 4
        filename = "#{@logdir}/#{jobname}.#{jobno}"
        logger = fs.createWriteStream filename
        logger.on 'open', () -> cb logger, jobno

    watchRunFile = (fn) =>
      fs.watch fn, {}, (eventType, filename) =>
        return if eventType isnt 'rename'
        @executors[id].cancel()


    jobnameToLogfileStream jobname, (fd, jobno) =>
      runFilename = "#{@rundir}/#{id}/#{jobname}.#{jobno}"
      fs.writeFile runFilename, cmd, () =>
        runFileWatcher = watchRunFile runFilename
        @executors[id].execute jobname, jobno, cmd, fd, (code) =>
          runFileWatcher.close()
          fs.unlink runFilename, () ->
            cb code



E = new Exec
  execdir: (process.env.EXEC_DIR or ".")
  workdir: (process.env.WORK_DIR or ".")
  shells: ['bash -c', 'bash -c']
E.start (err) ->
