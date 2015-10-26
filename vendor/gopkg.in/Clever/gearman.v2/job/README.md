# job
--
    import "gopkg.in/Clever/gearman.v2/job"


## Usage

#### type Job

```go
type Job struct {
}
```

Job represents a Gearman job

#### func  New

```go
func New(handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) *Job
```
New creates a new Gearman job with the specified handle, updating the job based
on the packets in the packets channel. The only packets coming down packets
should be packets for this job. It also takes in two WriteClosers to right job
data and warnings to.

#### func (Job) Handle

```go
func (j Job) Handle() string
```
Handle returns job handle

#### func (*Job) Run

```go
func (j *Job) Run() State
```
Run blocks until the job completes. Returns the state, Completed or Failed.

#### func (Job) Status

```go
func (j Job) Status() Status
```
Status returns the current status of the gearman job

#### type State

```go
type State int
```

State of a Gearman job

```go
const (
	// Unknown is the default 'State' that should not be encountered
	Unknown State = iota
	// Running means that the job has not yet finished
	Running
	// Completed means that the job finished successfully
	Completed
	// Failed means that the job failed
	Failed
)
```

#### func (State) String

```go
func (s State) String() string
```
String implements the fmt.Stringer interface for easy printing.

#### type Status

```go
type Status struct {
	// Numerator is the numerator of the % complete
	Numerator int
	// Denominator is the denominator of the % complete
	Denominator int
}
```

Status of a Gearman job
