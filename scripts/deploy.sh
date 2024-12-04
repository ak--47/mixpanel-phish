#!/bin/bash

# http deploy
gcloud alpha functions deploy mixpanel-phish \
	--gen2 \
	--no-allow-unauthenticated \
	--env-vars-file env.yaml \
	--runtime nodejs20 \
	--region us-central1 \
	--trigger-http \
	--memory 4GB \
	--entry-point entry \
	--source . \
	--timeout=3600 \
	--max-instances=100 \
	--min-instances=0 \
	--concurrency=1