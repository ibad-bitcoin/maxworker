package queue

//JobQueue ... a buffered channel that we can send work requests on.
var JobQueue chan Queuable

var MetricType string

//Queuable ... interface of Queuable Job
type Queuable interface {
    Handle() error
}

//Dispatcher ... worker dispatcher
type Dispatcher struct {
    maxWorkers int
    WorkerPool chan chan Queuable
    Workers    []Worker
}

func (d *Dispatcher) dispatch() {
    for {
        select {
        case job := <-JobQueue:

              // Increase running jobs Gauge
            RunningJobs.WithLabelValues(MetricType).Inc()

            // a job request has been received
            go func(job Queuable) {
                // try to obtain a worker job channel that is available.
                // this will block until a worker is idle
                jobChannel := <-d.WorkerPool

                // dispatch the job to the worker job channel
                jobChannel <- job
            }(job)
        }
    }
}  

//Run ... starts work of dispatcher and creates the workers
func (d *Dispatcher) Run() {
    // starting n number of workers
    for i := 0; i < d.maxWorkers; i++ {

             // increase the number of running workers
        RunningWorkers.WithLabelValues(MetricType).Inc()

        worker := NewWorker(d.WorkerPool)
        worker.Start()
        // register in dispatcher's workers
        d.Workers = append(d.Workers, worker)
    }

    go d.dispatch()
}

//NewDispatcher ... creates new queue dispatcher
func NewDispatcher(maxWorkers int) *Dispatcher {
    // make job bucket
    if JobQueue == nil {
        JobQueue = make(chan Queuable, 10)
    }
    pool := make(chan chan Queuable, maxWorkers)
    return &Dispatcher{WorkerPool: pool, maxWorkers: maxWorkers}
}