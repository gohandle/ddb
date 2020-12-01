package ddb

import (
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// withLocalDB will run local Dynamodb for the duration of the test while creating any tables
// that are provided
func withLocalDB(tb testing.TB, tbls ...*dynamodb.CreateTableInput) (ddb *dynamodb.DynamoDB) {
	jexe, err := exec.LookPath("java")
	if jexe == "" || err != nil {
		tb.Fatalf("java not available in PATH: %v", err)
	}

	port := rand.Intn(65535-49152+1) + 49152
	cmd := exec.Command(jexe,
		"-D"+filepath.Join("internal", "localddb", "DynamoDBLocal_lib"),
		"-jar", filepath.Join("internal", "localddb", "DynamoDBLocal.jar"),
		"-inMemory", "--port", strconv.Itoa(port),
	)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err = cmd.Start(); err != nil {
		tb.Fatalf("failed to start local ddb: %v", err)
	}

	tb.Cleanup(func() {
		cmd.Process.Kill()
	})

	var sess *session.Session
	if sess, err = session.NewSession(&aws.Config{
		Region:   aws.String("eu-west-1"),
		Endpoint: aws.String("http://localhost:" + strconv.Itoa(port)),
	}); err != nil {
		tb.Fatalf("failed to create local session: %v", err)
	}

	ddb = dynamodb.New(sess)
	for _, in := range tbls {
		if _, err = ddb.CreateTable(in); err != nil {
			tb.Fatalf("failed to create table %v: %v", in, err)
		}
	}

	return
}
