package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"
)

type FileFormat string

const (
	Parquet FileFormat = "parquet"
	CSV     FileFormat = "csv"
)

const TableName = "p" // p for preview

// FileNameString represents one or more file names enclosed in single quotes and separated by commas
type FileNameString string

func exitWithError(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
	os.Exit(1)
}

const version = "1.0.0"

var rootCmd = &cobra.Command{
	Use:     "dpi <file or pattern>",
	Short:   "DuckDB Parquet/CSV Inspector",
	Version: version,
	Long:    `DPI is a tool for inspecting Parquet and CSV files using DuckDB.`,
	Example: `  dpi data.parquet
  dpi *.parquet
  dpi data.csv
  dpi -s data.csv     # With strict mode for CSV`,
	Args: cobra.ExactArgs(1),
	Run:  runCommand,
}

func init() {
	rootCmd.Flags().BoolP("strict", "s", false, "Enable strict mode (for CSV files)")
}

func Execute() {
	// check if DuckDB binary is available
	if err := ensureDuckDBBinary(); err != nil {
		exitWithError("%v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		exitWithError("Command execution failed: %v", err)
	}
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func createTempDirectory() (string, error) {
	return os.MkdirTemp("", "dpi")
}

func determineFileFormat(filename string) FileFormat {
	ext := filepath.Ext(filename)
	switch strings.ToLower(ext) {
	case ".parquet":
		return Parquet
	case ".csv", ".gz":
		return CSV
	default:
		return "" // Unsupported format
	}
}

func createTemporaryTable(filename FileNameString, tempDir string, fileFormat FileFormat, strict bool) error {
	var query string

	switch fileFormat {
	case Parquet:
		query = fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_parquet([%s]);`,
			TableName, filename)
	case CSV:
		query = fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_csv(%s, strict_mode=%v);`,
			TableName, filename, strict)
	default:
		return fmt.Errorf("unsupported file format: %s", fileFormat)
	}

	duckdbPath := filepath.Join(tempDir, "tmp.duckdb")
	cmds := []string{
		"duckdb",
		duckdbPath,
		"-c",
		query,
	}

	if err := executeCommand(cmds); err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}
	return nil
}

func findParquetFiles(pattern string) ([]string, error) {
	files, err := filepath.Glob(pattern)
	if err != nil {
		// Glob only returns ErrBadPattern, which is unlikely with user input
		// but we'll handle it anyway
		return nil, fmt.Errorf("invalid file pattern '%s': %w", pattern, err)
	}
	return files, nil
}

func executeCommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func ensureDuckDBBinary() error {
	_, err := exec.LookPath("duckdb")
	if err != nil {
		return fmt.Errorf("DuckDB binary not found in system PATH. Please install DuckDB: https://duckdb.org/docs/installation/")
	}
	return nil
}

// setupSignalHandler sets up signal handling for graceful cleanup.
// It returns a cleanup function that should be deferred.
func setupSignalHandler() func() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Track whether we received a signal (using atomic for thread-safety)
	var signalReceived atomic.Bool

	// Start goroutine to handle signals
	go func() {
		sig := <-sigChan
		signalReceived.Store(true)
		fmt.Fprintf(os.Stderr, "\nReceived signal %v, waiting for DuckDB to exit...\n", sig)
		// Don't exit here - let DuckDB handle the signal and exit naturally
		// This allows our defer statements to run for cleanup
	}()

	// Return cleanup function
	return func() {
		if signalReceived.Load() {
			fmt.Fprintln(os.Stderr, "Cleanup completed, exiting")
			os.Exit(130) // Exit code 130 is conventional for SIGINT (128 + 2)
		}
	}
}

func runCommand(cmd *cobra.Command, args []string) {
	// Set up signal handler early to ensure cleanup happens even if interrupted
	cleanupSignalHandler := setupSignalHandler()
	defer cleanupSignalHandler()

	fmt.Fprintln(os.Stdout, "============== Initial dpi setup ==============")

	filePath := args[0]
	strict := cmd.Flag("strict").Value.String() == "true"

	// Determine file format
	fileFormat := determineFileFormat(filePath)
	if fileFormat == "" {
		exitWithError("Unsupported file format for file: %s", filePath)
	}
	fmt.Fprintf(os.Stdout, "Detected file format: %s\n", fileFormat)

	// Create temporary directory
	tempDir, err := createTempDirectory()
	if err != nil {
		exitWithError("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up the temporary directory after use
	fmt.Fprintf(os.Stdout, "Using temporary directory: %s\n", tempDir)

	// Process files based on format
	filename, err := processInputFiles(filePath, fileFormat)
	if err != nil {
		exitWithError("%v", err)
	}

	// Create temporary table
	if err := createTemporaryTable(filename, tempDir, fileFormat, strict); err != nil {
		exitWithError("Creating temporary table failed: %v", err)
	}
	fmt.Fprintln(os.Stdout, "Temporary table created successfully")

	// Start DuckDB CLI
	fmt.Fprintln(os.Stdout, "============== Starting DuckDB CLI ==============")
	duckdbPath := filepath.Join(tempDir, "tmp.duckdb")
	cmds := []string{"duckdb", duckdbPath}

	if err := executeCommand(cmds); err != nil {
		exitWithError("Failed to execute DuckDB: %v", err)
	}
}

func processInputFiles(filePath string, fileFormat FileFormat) (FileNameString, error) {
	if fileFormat == Parquet {
		// For Parquet files, handle multiple files using glob patterns
		files, err := findParquetFiles(filePath)
		if err != nil {
			return "", err
		}
		if len(files) == 0 {
			return "", fmt.Errorf("no Parquet files found matching pattern: %s", filePath)
		}

		// Create a single FileNameString by concatenating files separated by commas
		var filenames []string
		for _, f := range files {
			filenames = append(filenames, "'"+f+"'")
		}
		return FileNameString(strings.Join(filenames, ",")), nil
	} else {
		// For other file formats, check if file exists
		if !fileExists(filePath) {
			return "", fmt.Errorf("file does not exist: %s", filePath)
		}
		return FileNameString("'" + filePath + "'"), nil
	}
}
