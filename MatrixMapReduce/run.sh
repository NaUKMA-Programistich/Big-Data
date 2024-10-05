#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display a separator
separator() {
    echo "-----------"
}

# Matrix Dimensions
MATRIX_A_ROWS=3
MATRIX_A_COLUMNS_B_ROWS=3
MATRIX_B_COLUMNS=2

# Variables
CONTAINER_NAME="namenode"                                 # Name of your Docker container
LOCAL_INPUT_DIR="./input"                                 # Local input directory containing input files
CONTAINER_TMP_DIR="/tmp/matrix/input"                     # Temporary directory inside the container for input files
CONTAINER_TMP_DIR_OUTPUT="/tmp/matrix/output"                     # Temporary directory inside the container for input files
HDFS_INPUT_DIR="matrix/input"                             # HDFS directory for input files
HDFS_OUTPUT_DIR="matrix/output"                           # HDFS directory for output files
LOCAL_OUTPUT_DIR="./output"                               # Local directory to store output files
JAR_LOCAL_PATH="build/libs/MatrixMapReduce-1.0.jar"       # Path to the built JAR file
JAR_CONTAINER_PATH="/tmp/matrix/MatrixMapReduce-1.0.jar"  # Path inside the container to store the JAR

# Ensure local input directory exists
if [ ! -d "$LOCAL_INPUT_DIR" ]; then
    echo "Error: Local input directory '$LOCAL_INPUT_DIR' does not exist."
    echo "Please create it and add your input files."
    exit 1
fi

# Step 1: Create /tmp/matrix/input directory inside the Docker container
echo "Creating temporary input directory inside the Docker container..."
docker exec "$CONTAINER_NAME" bash -c "mkdir -p $CONTAINER_TMP_DIR"
separator

# Step 2: Copy all input files from local ./input/ to /tmp/matrix/input/ in the container
echo "Copying input files from local '$LOCAL_INPUT_DIR' to container '$CONTAINER_NAME:$CONTAINER_TMP_DIR'..."
docker cp "$LOCAL_INPUT_DIR"/. "$CONTAINER_NAME":"$CONTAINER_TMP_DIR"/
separator

# Step 3: Remove existing HDFS 'matrix/input' directory if it exists
echo "Removing existing HDFS directory '$HDFS_INPUT_DIR' if it exists..."
docker exec "$CONTAINER_NAME" bash -c "hadoop fs -rm -r -f $HDFS_INPUT_DIR"
separator

# Step 4: Create HDFS 'matrix/input' directory
echo "Creating HDFS directory '$HDFS_INPUT_DIR'..."
docker exec "$CONTAINER_NAME" bash -c "hadoop fs -mkdir -p $HDFS_INPUT_DIR"
separator

# Step 5: Put all input files from container's /tmp/matrix/input/ to HDFS 'matrix/input'
echo "Uploading input files to HDFS directory '$HDFS_INPUT_DIR'..."
docker exec "$CONTAINER_NAME" bash -c "hadoop fs -put $CONTAINER_TMP_DIR/* $HDFS_INPUT_DIR"
separator

# Step 6: Remove existing HDFS 'matrix/output' directory if it exists
echo "Removing existing HDFS directory '$HDFS_OUTPUT_DIR' if it exists..."
docker exec "$CONTAINER_NAME" bash -c "hadoop fs -rm -r -f $HDFS_OUTPUT_DIR"
separator

# Step 7: Build the Kotlin project using Gradle
echo "Building the Kotlin project with Gradle..."
./gradlew clean shadowJar
echo "Gradle build completed successfully."
separator

# Step 8: Copy the built JAR to the Docker container
echo "Copying the JAR file to the Docker container..."
docker cp "$JAR_LOCAL_PATH" "$CONTAINER_NAME":"$JAR_CONTAINER_PATH"
separator

# Step 9: Run the Hadoop MapReduce job
echo "Running the Hadoop MapReduce job..."
docker exec "$CONTAINER_NAME" bash -c "hadoop jar $JAR_CONTAINER_PATH MatrixMapReduce $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR $MATRIX_A_ROWS $MATRIX_A_COLUMNS_B_ROWS $MATRIX_B_COLUMNS"
separator

# Step 10: Clean existing output directory inside the container to prevent 'File exists' errors
echo "Cleaning existing output directory inside the container..."
docker exec "$CONTAINER_NAME" bash -c "rm -rf '$CONTAINER_TMP_DIR_OUTPUT'"
separator

# Step 11: Retrieve the output from HDFS to the container's /tmp/matrix/output directory
echo "Retrieving output from HDFS..."
# Ensure the destination directory is clean
docker exec "$CONTAINER_NAME" bash -c "rm -rf '$CONTAINER_TMP_DIR_OUTPUT'"
# Retrieve the output
docker exec "$CONTAINER_NAME" bash -c "hadoop fs -get '$HDFS_OUTPUT_DIR' '$CONTAINER_TMP_DIR_OUTPUT'"
separator

# Step 13: Copy the output from the container to the local ./output directory
echo "Copying output files from container to local '$LOCAL_OUTPUT_DIR' directory..."
docker cp "$CONTAINER_NAME":"$CONTAINER_TMP_DIR_OUTPUT" "$LOCAL_OUTPUT_DIR"
separator

echo "Matrix multiplication MapReduce job completed successfully."
echo "Output files are available in the '$LOCAL_OUTPUT_DIR' directory."
separator
