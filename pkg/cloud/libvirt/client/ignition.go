package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/golang/glog"
	libvirtxml "github.com/libvirt/libvirt-go-xml"
	providerconfigv1 "github.com/openshift/cluster-api-provider-libvirt/pkg/apis/libvirtproviderconfig/v1beta1"
	"github.com/pkg/errors"
)

func setIgnition(domainDef *libvirtxml.Domain, client *libvirtClient, ignition *providerconfigv1.Ignition, kubeClient kubernetes.Interface, machineNamespace, volumeName string) error {
	glog.Info("Creating ignition file")
	ignitionDef := newIgnitionDef()

	if ignition.UserDataSecret == "" {
		return fmt.Errorf("ignition.userDataSecret not set")
	}

	secret, err := kubeClient.CoreV1().Secrets(machineNamespace).Get(ignition.UserDataSecret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("can not retrieve user data secret '%v/%v' when constructing cloud init volume: %v", machineNamespace, ignition.UserDataSecret, err)
	}
	userDataSecret, ok := secret.Data["userData"]
	if !ok {
		return fmt.Errorf("can not retrieve user data secret '%v/%v' when constructing cloud init volume: key 'userData' not found in the secret", machineNamespace, ignition.UserDataSecret)
	}

	ignitionDef.Name = volumeName
	ignitionDef.PoolName = client.poolName
	ignitionDef.Content = string(userDataSecret)

	glog.Infof("Ignition: %+v", ignitionDef)

	ignitionVolumeName, err := ignitionDef.createAndUpload(client)
	if err != nil {
		return err
	}

	domainDef.QEMUCommandline = &libvirtxml.DomainQEMUCommandline{
		Args: []libvirtxml.DomainQEMUCommandlineArg{
			{
				// https://github.com/qemu/qemu/blob/master/docs/specs/fw_cfg.txt
				Value: "-fw_cfg",
			},
			{
				Value: fmt.Sprintf("name=opt/com.coreos/config,file=%s", ignitionVolumeName),
			},
		},
	}
	return nil
}

func setIgnitionForS390X(domainDef *libvirtxml.Domain, client *libvirtClient, ignition *providerconfigv1.Ignition, kubeClient kubernetes.Interface, machineNamespace, volumeName string) error {
	glog.Info("Creating ignition file for s390x")
	ignitionDef := newIgnitionDef()

	if ignition.UserDataSecret == "" {
		return fmt.Errorf("ignition.userDataSecret not set")
	}

	secret, err := kubeClient.CoreV1().Secrets(machineNamespace).Get(ignition.UserDataSecret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("can not retrieve user data secret '%v/%v' when constructing cloud init volume: %v", machineNamespace, ignition.UserDataSecret, err)
	}
	userDataSecret, ok := secret.Data["userData"]
	if !ok {
		return fmt.Errorf("can not retrieve user data secret '%v/%v' when constructing cloud init volume: key 'userData' not found in the secret", machineNamespace, ignition.UserDataSecret)
	}

	ignitionDef.Name = volumeName
	ignitionDef.PoolName = client.poolName
	ignitionDef.Content = string(userDataSecret)

	glog.Infof("Ignition: %+v", ignitionDef)

	ignitionVolumeName, err := ignitionDef.createAndUpload(client)
	if err != nil {
		return err
	}

	// _fw_cfg isn't supported on s390x, so we use guestfish to inject the ignition for now
	return injectIgnitionByGuestfish(domainDef, ignitionVolumeName)
}

func injectIgnitionByGuestfish(domainDef *libvirtxml.Domain, ignitionFile string) error {
	glog.Info("Injecting ignition configuration using guestfish")

	/*
	 * Add the image into guestfish, execute the following command,
	 *     guestfish --listen -a ${volumeFilePath}
	 *
	 * output example:
	 *  	   GUESTFISH_PID=4513; export GUESTFISH_PID
	 */
	args := []string{"--listen", "-a", domainDef.Devices.Disks[0].Source.File.File}
	output, err := startCmd(args...)
	if err != nil {
		return err
	}

	strArray := strings.Split(output, ";")
	if len(strArray) != 2 {
		return fmt.Errorf("Invalid output when starting guestfish: %s", output)
	}
	strArray1 := strings.Split(strArray[0], "=")
	if len(strArray) != 2 {
		return fmt.Errorf("failed to get the guestfish PID from %s", output)
	}
	os.Setenv(strArray1[0], strArray1[1])

	/*
	 * Launch guestfish, execute the following command,
	 *     guestfish --remote -- run
	 */
	args = []string{"--remote", "--", "run"}
	_, err = execCmd(args...)
	if err != nil {
		return err
	}

	/*
	 * Get the boot filesystem, execute the following command,
	 *     guestfish --remote -- list-filesystems
	 *
	 *	output example:
	 *		/dev/sda1: ext4
	 *		/dev/sda2: vfat
	 *		/dev/sda3: unknown
	 *		/dev/sda4: xfs
	 */
	args = []string{"--remote", "--", "list-filesystems"}
	output, err = execCmd(args...)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	_, err = buf.WriteString(output)
	if err != nil {
		return fmt.Errorf("failed to save the filesystems list, %s", err)
	}

	var bootDisk string
	for {
		line, err := buf.ReadString(byte('\n'))
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to read a line from filesystem list, %s", err)
		}

		diskAndFormat := strings.Split(line, ":")
		if len(diskAndFormat) != 2 {
			return fmt.Errorf("invalid filesystem format: %s", line)
		}

		if diskAndFormat[1] == "ext4" {
			bootDisk = diskAndFormat[0]
			break
		}
	}

	/*
	 * Mount the boot filesystem, execute the following command,
	 *     guestfish --remote -- mount ${boot_filesystem} /
	 */
	args = []string{"--remote", "--", "mount", bootDisk, "/"}
	_, err = execCmd(args...)
	if err != nil {
		return err
	}

	/*
	 * Upload the ignition file, execute the following command,
	 *     guestfish --remote -- upload ${ignition_filepath} /ignition/config.ign
	 *
	 * The target path is hard coded as "/ignition/config.ign" for now
	 */
	args = []string{"--remote", "--", "upload", ignitionFile, "/ignition/config.ign"}
	_, err = execCmd(args...)
	if err != nil {
		return err
	}

	/*
	 * Umount all filesystems, execute the following command,
	 *     guestfish --remote -- umount-all
	 */
	args = []string{"--remote", "--", "umount-all"}
	_, err = execCmd(args...)
	if err != nil {
		return err
	}

	/*
	 * Exit guestfish, execute the following command,
	 *     guestfish --remote -- exit
	 */
	args = []string{"--remote", "--", "exit"}
	_, err = execCmd(args...)
	if err != nil {
		return err
	}

	return nil
}

func execCmd(args ...string) (string, error) {
	const executable = "guestfish"
	glog.Infof("Running: %v %v", executable, args)
	cmd := exec.Command(executable, args...)
	cmdOut, err := cmd.CombinedOutput()
	glog.Infof("Ran: %v %v Output: %v", executable, args, string(cmdOut))
	if err != nil {
		err = errors.Wrapf(err, "error running command '%v %v'", executable, strings.Join(args, " "))
	}
	return string(cmdOut), err
}

// startCmd starts the command, and doesn't wait for it to complete
func startCmd(args ...string) (string, error) {
	const executable = "guestfish"
	glog.Infof("Starting: %v %v", executable, args)
	cmd := exec.Command(executable, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", errors.Wrapf(err, "error getting stdout pipe for command '%v %v'", executable, strings.Join(args, " "))
	}
	err = cmd.Start()
	glog.Infof("Started: %v %v", executable, args)
	if err != nil {
		return "", errors.Wrapf(err, "error starting command '%v %v'", executable, strings.Join(args, " "))
	}

	outMsg, err := readOutput(stdout)
	glog.Infof("output message: %s", outMsg)

	return outMsg, err
}

func readOutput(stream io.ReadCloser) (string, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(stream)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

type defIgnition struct {
	Name     string
	PoolName string
	Content  string
}

// Creates a new cloudinit with the defaults
// the provider uses
func newIgnitionDef() defIgnition {
	return defIgnition{}
}

// Create a ISO file based on the contents of the CloudInit instance and
// uploads it to the libVirt pool
// Returns a string holding terraform's internal ID of this resource
func (ign *defIgnition) createAndUpload(client *libvirtClient) (string, error) {
	volumeDef := newDefVolume(ign.Name)

	ignFile, err := ign.createFile()
	if err != nil {
		return "", err
	}
	defer func() {
		// Remove the tmp ignition file
		if err = os.Remove(ignFile); err != nil {
			glog.Infof("Error while removing tmp Ignition file: %s", err)
		}
	}()

	img, err := newImage(ignFile)
	if err != nil {
		return "", err
	}

	size, err := img.size()
	if err != nil {
		return "", err
	}

	volumeDef.Capacity.Unit = "B"
	volumeDef.Capacity.Value = size
	volumeDef.Target.Format.Type = "raw"

	return uploadVolume(ign.PoolName, client, volumeDef, img)

}

// Dumps the Ignition object to a temporary ignition file
func (ign *defIgnition) createFile() (string, error) {
	glog.Info("Creating Ignition temporary file")
	tempFile, err := ioutil.TempFile("", ign.Name)
	if err != nil {
		return "", fmt.Errorf("Cannot create tmp file for Ignition: %s",
			err)
	}
	defer tempFile.Close()

	var file bool
	file = true
	if _, err := os.Stat(ign.Content); err != nil {
		var js map[string]interface{}
		if errConf := json.Unmarshal([]byte(ign.Content), &js); errConf != nil {
			return "", fmt.Errorf("coreos_ignition 'content' is neither a file "+
				"nor a valid json object %s", ign.Content)
		}
		file = false
	}

	if !file {
		if _, err := tempFile.WriteString(ign.Content); err != nil {
			return "", fmt.Errorf("Cannot write Ignition object to temporary " +
				"ignition file")
		}
	} else if file {
		ignFile, err := os.Open(ign.Content)
		if err != nil {
			return "", fmt.Errorf("Error opening supplied Ignition file %s", ign.Content)
		}
		defer ignFile.Close()
		_, err = io.Copy(tempFile, ignFile)
		if err != nil {
			return "", fmt.Errorf("Error copying supplied Igition file to temporary file: %s", ign.Content)
		}
	}
	return tempFile.Name(), nil
}
