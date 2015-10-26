package job

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"gopkg.in/Clever/gearman.v2/packet"
)

// State of a Gearman job
type State int

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

// String implements the fmt.Stringer interface for easy printing.
func (s State) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Running:
		return "Running"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	}
	return "Unknown"
}

// Status of a Gearman job
type Status struct {
	// Numerator is the numerator of the % complete
	Numerator int
	// Denominator is the denominator of the % complete
	Denominator int
}

// Job represents a Gearman job
type Job struct {
	handle         string
	data, warnings io.WriteCloser
	status         Status
	state          State
	done           chan struct{}
}

// Handle returns job handle
func (j Job) Handle() string {
	return j.handle
}

// Status returns the current status of the gearman job
func (j Job) Status() Status {
	return j.status
}

// Run blocks until the job completes. Returns the state, Completed or Failed.
func (j *Job) Run() State {
	<-j.done
	return j.state
}

// handlePackets updates a job based off of incoming packets associated with this job.
func (j *Job) handlePackets(packets <-chan *packet.Packet) {
	for pack := range packets {
		switch pack.Type {
		case packet.WorkStatus:
			// check that packet is valid WORK_STATUS
			if len(pack.Arguments) != 3 {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Recieved invalid WORK_STATUS packet with '%d' fields\n",
					len(pack.Arguments))
				return
			}

			num, err := strconv.Atoi(string(pack.Arguments[1]))
			if err != nil {
				fmt.Fprintln(os.Stderr, "GEARMAN WARNING: Error converting numerator", err)
			}
			den, err := strconv.Atoi(string(pack.Arguments[2]))
			if err != nil {
				fmt.Fprintln(os.Stderr, "GEARMAN WARNING: Error converting denominator", err)
			}
			j.status = Status{Numerator: num, Denominator: den}
		case packet.WorkComplete:
			j.state = Completed
			close(j.done)
		case packet.WorkFail:
			j.state = Failed
			close(j.done)
		case packet.WorkData:
			if _, err := j.data.Write(pack.Arguments[1]); err != nil {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Error writing data, arg: %s, err: %s",
					pack.Arguments[1], err)
			}
		case packet.WorkWarning:
			if _, err := j.warnings.Write(pack.Arguments[1]); err != nil {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Error writing warnings, arg: %s, err: %s",
					pack.Arguments[1], err)
			}
		default:
			fmt.Fprintln(os.Stderr, "GEARMAN WARNING: Unimplemented packet type", pack.Type)
		}
	}
}

// New creates a new Gearman job with the specified handle, updating the job based on the packets
// in the packets channel. The only packets coming down packets should be packets for this job.
// It also takes in two WriteClosers to right job data and warnings to.
func New(handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) *Job {
	j := &Job{
		handle:   handle,
		data:     data,
		warnings: warnings,
		status:   Status{},
		state:    Running,
		done:     make(chan struct{}),
	}
	go j.handlePackets(packets)
	return j
}
