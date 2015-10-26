package gearman

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"sync"

	"gopkg.in/Clever/gearman.v2/job"
	"gopkg.in/Clever/gearman.v2/packet"
	"gopkg.in/Clever/gearman.v2/scanner"
)

// noOpCloser is like an ioutil.NopCloser, but for an io.Writer.
type noOpCloser struct {
	w io.Writer
}

func (c noOpCloser) Write(data []byte) (n int, err error) {
	return c.w.Write(data)
}

func (c noOpCloser) Close() error {
	return nil
}

var discard = noOpCloser{w: ioutil.Discard}

type partialJob struct {
	// data is used to write data back to the caller's provided io.Writer
	data io.WriteCloser
	// warnings is used to write warning messages back to the caller's provided io.Writer
	warnings io.WriteCloser
}

// Client is a Gearman client
type Client struct {
	// conn is the connection to the gearman server
	conn io.WriteCloser
	// packets is the stream of incoming gearman packets from the server
	packets chan *packet.Packet
	// jobs is a router for sending packets to the correct job to interpret
	jobs map[string]chan *packet.Packet
	// partialJobs
	partialJobs chan *partialJob
	newJobs     chan *job.Job
	jobLock     sync.RWMutex
}

// Close terminates the connection to the server
func (c *Client) Close() error {
	// TODO: figure out when to close packet chan
	return c.conn.Close()
}

func (c *Client) submit(fn string, payload []byte, data, warnings io.WriteCloser, t packet.Type) (*job.Job, error) {
	// create and marshal the gearman packet
	pack := &packet.Packet{
		Code:      packet.Req,
		Type:      t,
		Arguments: [][]byte{[]byte(fn), []byte{}, payload},
	}
	buf, err := pack.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// write the packet to the gearman server
	if _, err := io.Copy(c.conn, bytes.NewBuffer(buf)); err != nil {
		return nil, err
	}

	// block while the client waits for confirmation that a job has been created
	c.partialJobs <- &partialJob{data: data, warnings: warnings}
	return <-c.newJobs, nil
}

// Submit sends a new job to the server with the specified function and payload. You must provide
// two WriteClosers for data and warnings to be written to.
func (c *Client) Submit(fn string, payload []byte, data, warnings io.WriteCloser) (*job.Job, error) {
	return c.submit(fn, payload, data, warnings, packet.SubmitJob)
}

// SubmitBackground submits a background job. There is no access to data, warnings, or completion
// state.
func (c *Client) SubmitBackground(fn string, payload []byte) error {
	_, err := c.submit(fn, payload, discard, discard, packet.SubmitJobBg)
	return err
}

// addJob adds the reference to a job and its packet stream to the internal map of packet streams.
func (c *Client) addJob(handle string, packets chan *packet.Packet) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	c.jobs[handle] = packets
}

// getJob returns the reference to channel for a specific job based off of its handle.
func (c *Client) getJob(handle string) chan *packet.Packet {
	c.jobLock.RLock()
	defer c.jobLock.RUnlock()
	return c.jobs[handle]
}

// deleteJob removes a job's packet stream from the internal map of ongoing jobs.
func (c *Client) deleteJob(handle string) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	delete(c.jobs, handle)
}

// read attempts to read incoming packets from the gearman server to route them to the job
// they are intended for.
func (c *Client) read(scanner *bufio.Scanner) {
	for scanner.Scan() {
		pack := &packet.Packet{}
		if err := pack.UnmarshalBinary(scanner.Bytes()); err != nil {
			fmt.Fprintf(os.Stderr, "GEARMAN WARNING: error parsing packet! %#v\n", err)
		} else {
			c.packets <- pack
		}
	}
	if scanner.Err() != nil {
		fmt.Fprintf(os.Stderr, "GEARMAN WARNING: error scanning! %#v\n", scanner.Err())
	}
}

// routePackets forwards incoming packets to the correct job.
func (c *Client) routePackets() {
	// operate on every packet that has been read
	for pack := range c.packets {
		if len(pack.Arguments) == 0 {
			fmt.Fprintln(os.Stderr, "GEARMAN WARNING: packet read with no handle!")
			continue
		}

		handle := string(pack.Arguments[0])
		switch pack.Type {
		case packet.JobCreated:
			// create a new channel to send packets for this job
			packets := make(chan *packet.Packet)
			// optimistically hope that the last job submitted is the same one that just started
			pj := <-c.partialJobs
			// hook up the job to its packet stream
			j := job.New(handle, pj.data, pj.warnings, packets)
			// add the packet stream to the internal routing map
			c.addJob(handle, packets)
			// finally unblock the Submit() fn call
			c.newJobs <- j

			go func() {
				defer close(packets)
				defer c.deleteJob(handle)
				j.Run()
			}()
		default:
			// send the packet to the right job
			pktStream := c.getJob(handle)
			if pktStream != nil {
				pktStream <- pack
			} else {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: packet read with handle of '%s', "+
					"no reference found in client.!\n", handle)
			}
		}
	}
}

// NewClient returns a new Gearman client pointing at the specified server
func NewClient(network, addr string) (*Client, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("Error while establishing a connection to gearman: %s", err)
	}

	c := &Client{
		conn:        conn,
		packets:     make(chan *packet.Packet),
		newJobs:     make(chan *job.Job),
		partialJobs: make(chan *partialJob),
		jobs:        make(map[string]chan *packet.Packet),
	}
	go c.read(scanner.New(conn))
	go c.routePackets()

	return c, nil
}
