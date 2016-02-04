package aws

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

const (
	saltedTrustyAMI = "ami-45aa5901"
	devSubnet       = "private_west1a_staging"
)

type Client struct {
	client *ec2.EC2
}

type Instance struct {
	InstanceID     string
	PrivateDNSName string
	VolumeID       string
}

func NewClient(region string) *Client {
	svc := ec2.New(session.New(&aws.Config{Region: aws.String(region)}))
	return &Client{client: svc}
}

func (c *Client) TerminateInstance(instanceID string) error {
	input := ec2.TerminateInstancesInput{
		InstanceIds: []*string{&instanceID},
	}
	_, err := c.client.TerminateInstances(&input)
	return err
}

func (c *Client) FindSnapshot(snapshotID string) (*ec2.Snapshot, error) {
	input := ec2.DescribeSnapshotsInput{
		Filters: []*ec2.Filter{
			&ec2.Filter{
				Name:   aws.String("snapshot-id"),
				Values: []*string{&snapshotID},
			},
		},
	}

	response, err := c.client.DescribeSnapshots(&input)
	if err != nil {
		return nil, err
	}

	if len(response.Snapshots) != 1 {
		return nil, errors.New("incorrect number of snapshots return")
	}
	return response.Snapshots[0], nil
}
