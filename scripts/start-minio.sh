#!/bin/bash
# Helper script to start MinIO for testing

echo "Starting MinIO for Stowage testing..."

docker run -d -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  --name stowage-minio \
  minio/minio server /data --console-address ":9001"

if [ $? -eq 0 ]; then
    echo "✅ MinIO started successfully!"
    echo "   API:     http://localhost:9000"
    echo "   Console: http://localhost:9001"
    echo "   Login:   minioadmin / minioadmin"
    echo ""
    echo "To stop: docker stop stowage-minio"
    echo "To remove: docker rm stowage-minio"
else
    echo "❌ Failed to start MinIO"
    echo "It may already be running. Try: docker ps | grep minio"
fi
