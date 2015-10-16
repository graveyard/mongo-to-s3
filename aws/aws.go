package aws

import (
	"errors"
	"log"

	"github.com/aws/aws-sdk-go/aws"
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
	config := aws.NewConfig().WithRegion(region)
	svc := ec2.New(config)
	return &Client{client: svc}
}

func (c *Client) TerminateInstance(instanceID string) error {
	input := ec2.TerminateInstancesInput{
		InstanceIds: []*string{&instanceID},
	}
	_, err := c.client.TerminateInstances(&input)
	return err
}

// TODO: Figure out how to handle cleanup if a later request fails
func (c *Client) RunInstanceFromLatestSnapshot(description string) (*Instance, error) {
	snapshots, err := c.FindSnapshots(description)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, errors.New("No Snapshots found!")
	}

	var latestSnapshot *ec2.Snapshot
	for _, snapshot := range snapshots {
		if latestSnapshot == nil {
			latestSnapshot = snapshot
		} else if latestSnapshot.StartTime.Before(*snapshot.StartTime) {
			latestSnapshot = snapshot
		}
	}

	log.Println("latest snapshot time: ", latestSnapshot.StartTime)
	for _, tag := range latestSnapshot.Tags {
		log.Println("tag: ", tag.String())
	}
	_, err = c.CreateVolume(latestSnapshot, 300)
	if err != nil {
		return nil, err
	}

	// TODO: wait for volume to be created?

	_, err = c.RunInstance()
	if err != nil {
		return nil, err
	}
	// TODO: wait for instance to start

	//	attachment, err := c.AttachVolumeToInstance(volume, instance)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	return &Instance{*attachment.InstanceId, *instance.PrivateDnsName, *attachment.VolumeId}, nil
	//	// TODO: wait for attachment to complete
	log.Fatal("made it!")
	return nil, nil
}

// Find Snapshots returns a list of completed snapshots with a tag matching
// the description
// description = mongo-clever for SIS DB
func (c *Client) FindSnapshots(description string) ([]*ec2.Snapshot, error) {
	input := &ec2.DescribeSnapshotsInput{
		Filters: []*ec2.Filter{
			&ec2.Filter{
				Name:   aws.String("tag-value"),
				Values: aws.StringSlice([]string{"mongodb-clever"}),
			}, {
				Name:   aws.String("status"),
				Values: aws.StringSlice([]string{"completed"}),
			},
		},
	}

	output, err := c.client.DescribeSnapshots(input)
	return output.Snapshots, err
}

func (c *Client) CreateVolume(snapshot *ec2.Snapshot, size int64) (*ec2.Volume, error) {
	zonesResult, err := c.client.DescribeAvailabilityZones(nil)
	if err != nil {
		return nil, err
	}
	if len(zonesResult.AvailabilityZones) == 0 {
		return nil, errors.New("No Availability Zones found for Volume.")
	}

	input := &ec2.CreateVolumeInput{
		AvailabilityZone: zonesResult.AvailabilityZones[0].ZoneName,
		Size:             aws.Int64(size),
		DryRun:           aws.Bool(true),
	}
	if snapshot != nil {
		input.SnapshotId = snapshot.SnapshotId
	}

	return c.client.CreateVolume(input)
}

// TODO - change RunInstance to take instanceType as input
func (c *Client) RunInstance() (*ec2.Instance, error) {
	request := ec2.RunInstancesInput{
		DryRun:       aws.Bool(true),
		ImageId:      aws.String(saltedTrustyAMI),
		InstanceType: aws.String(ec2.InstanceTypeM3Large),
		MinCount:     aws.Int64(1),
		MaxCount:     aws.Int64(1),
		//SubnetId:     aws.String(devSubnet),
	}

	reservation, err := c.client.RunInstances(&request)
	if err != nil {
		return nil, err
	}

	// minCount/maxCount ensures that if no err is returned, we have 1 instance
	return reservation.Instances[0], err
}

func (c *Client) AttachVolumeToInstance(volume *ec2.Volume, instance *ec2.Instance) (*ec2.VolumeAttachment, error) {
	input := &ec2.AttachVolumeInput{
		VolumeId:   volume.VolumeId,
		InstanceId: instance.InstanceId,
	}

	return c.client.AttachVolume(input)
}
