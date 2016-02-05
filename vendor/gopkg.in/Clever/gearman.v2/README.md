# gearman
--
    import "gopkg.in/Clever/gearman.v2"

Package gearman provides a thread-safe Gearman client


### Example

Here's an example program that submits a job to Gearman and listens for events
from that job:

    package main

    import(
    	"gopkg.in/Clever/gearman.v2"
    	"ioutil"
    )

    func main() {
    	client, err := gearman.NewClient("tcp4", "localhost:4730")
    	if err != nil {
    		panic(err)
    	}

    	j, err := client.Submit("reverse", []byte("hello world!"), nil, nil)
    	if err != nil {
    		panic(err)
    	}
    	state := j.Run()
    	println(state) // job.Completed
    	data, err := ioutil.ReadAll(j.Data())
    	if err != nil {
    		panic(err)
    	}
    	println(data) // !dlrow olleh
    }

## Usage

#### type Client

```go
type Client struct {
}
```

Client is a Gearman client

#### func  NewClient

```go
func NewClient(network, addr string) (*Client, error)
```
NewClient returns a new Gearman client pointing at the specified server

#### func (*Client) Close

```go
func (c *Client) Close() error
```
Close terminates the connection to the server

#### func (*Client) Submit

```go
func (c *Client) Submit(fn string, payload []byte, data, warnings io.WriteCloser) (*job.Job, error)
```
Submit sends a new job to the server with the specified function and payload.
You must provide two WriteClosers for data and warnings to be written to.

#### func (*Client) SubmitBackground

```go
func (c *Client) SubmitBackground(fn string, payload []byte) error
```
SubmitBackground submits a background job. There is no access to data, warnings,
or completion state.
