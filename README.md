# coachPhoneCallDataTool
# Coach Phone Call Data Tool

This repository provides a proof of concept (POC) for analyzing phone call data using **Large Language Models (LLMs)**. The tool is designed to transcribe call recordings, store the transcriptions in **BigQuery**, and analyze the transcriptions to label each call using **Gemini LLMs** in **Vertex AI**.

---

## Repository Structure

The repository is organized into the following folders:

### 1. **dataflow_pipeline**
This folder contains the pipeline for processing and transcribing call recordings.

#### Files:
- **`main.py`**  
  Contains the **Google Dataflow** code to:
  - Read call recordings from **Google Cloud Storage (GCS)**.
  - Transcribe recordings using **Google Speech-to-Text**.
  - Write the transcriptions to **BigQuery**.

- **`cloudbuild.yaml`**  
  Used to automatically build and deploy the pipeline on **Google Cloud**.

- **`requirements.txt`**  
  Lists the Python dependencies for the Dataflow pipeline.

---

### 2. **llm_analysis_poc**
This folder contains the logic for analyzing transcriptions using **Gemini LLMs**.

#### Files:
- **`transcript_data_analysis.ipynb`**  
  A Jupyter Notebook that:
  - Connects to **BigQuery** to access the transcribed data.
  - Uses **Gemini LLMs** in **Vertex AI** to analyze the transcriptions.
  - Labels each call based on the analysis.

---

## How It Works

1. **Dataflow Pipeline**  
   The Dataflow pipeline takes call recordings stored in **GCS**, transcribes them using **Google Speech-to-Text**, and stores the results in **BigQuery** for further analysis.

2. **Transcription Analysis**  
   The Jupyter Notebook leverages **Gemini LLMs** via **Vertex AI** to analyze the transcriptions in BigQuery. The tool then outputs meaningful labels for each call based on the analysis.

---

## Setup Instructions

### Prerequisites
- **Google Cloud Platform (GCP)** account with access to:
  - **Cloud Storage**
  - **Dataflow**
  - **BigQuery**
  - **Vertex AI**
- Python 3.9 or later.

---

### Setting Up the Dataflow Pipeline

1. Install the dependencies:
   ```bash
   pip install -r dataflow_pipeline/requirements.txt
