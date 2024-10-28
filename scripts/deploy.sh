#!/bin/bash

# cloud event deploy
gcloud alpha functions deploy mcd-pos-pipeline \
  --runtime nodejs20 \
  --gen2 \
  --trigger-resource mixpanel-mcd-pos-poc \
  --trigger-event google.storage.object.finalize \
  --trigger-location us \
  --entry-point mp-pipeline \
  --env-vars-file env.yaml \
  --timeout=540 \
  --memory=2G



# http deploy
gcloud alpha functions deploy my-func-name \
	--gen2 \
	--no-allow-unauthenticated \
	--env-vars-file .env.yaml \
	--runtime nodejs20 \
	--region us-central1 \
	--trigger-http \
	--memory 4GB \
	--entry-point entry \
	--source ./dist/internal/ \
	--timeout=3600 \
	--max-instances=100 \
	--min-instances=0 \
	--concurrency=1