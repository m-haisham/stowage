#!/bin/bash
# Helper script to stop MinIO

echo "Stopping MinIO..."
docker stop stowage-minio
docker rm stowage-minio
echo "✅ MinIO stopped and removed"
