from pydub import AudioSegment
import pandas as pd
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timedelta, date
import apache_beam as beam
import json
import logging
import requests
from time import sleep
from google.cloud import storage
from os import path
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from google.cloud.exceptions import NotFound
from google.cloud import speech
import wave
import os
import subprocess
import sys
import re


logging.info ("start time" + datetime.now ().strftime ("%H:%M:%S"))

class processAudioFile(beam.DoFn):

    def __init__(self, phrases_model,env):
        self.phrases_model = phrases_model
        self.env = env

    def process(self, element):
        # logging.info('pipeline started running')
        # ENV = "PROD" #PROD/TEST
        BUCKET_NAME = "phone_recordings_bucket"

        if element.endswith(".mp4") : 
        
            recieved_file_name = element[:-4].split("/")[-1] #Right trims the .mp4 resulting in the file name
            mp4_file_uri = f"{BUCKET_NAME}/S3_files_recordings_sink/{element}"
            
            if self.env == "PROD":
                file_uri_wav = f"wav_files/{element.replace('.mp4','.wav')}"

            else:
                file_uri_wav = f"test_wav_files/{element.replace('.mp4','.wav')}"

            wav_file = f"{BUCKET_NAME}/{file_uri_wav}"
            match = re.match(r".*?_([\d]+)_VOICE.*", mp4_file_uri)
            call_id = str(match.group(1))

            logging.info(f"wav file uri : {file_uri_wav}")
            logging.info(f"Downloading video file: {recieved_file_name} to local cloud function environment")
            local_path,file_path = self.download_from_gcs(BUCKET_NAME , file_uri_wav)

            logging.info("Processing audio file to get the metadata ...")
            conversation_duration = self.get_audio_metadata(file_path)

            #Push the audio file to gcs bucket
            audio_uri = f"gs://{BUCKET_NAME}/{file_uri_wav}"    
            os.remove(file_path)
            logging.info(f"phrases model used is {self.phrases_model}")
            logging.info("Transcribing audio ...")
            results = self.transribe_audio(audio_uri,self.phrases_model)

            list_dict_transcribe,word_count,number_of_speakers = self.process_results_to_dict(results,wav_file,call_id)

            date_transcribed = datetime.now()

            # get download date of vide file
            download_datetime = self.get_videofile_gcs_upload_time(f"S3_files_recordings_sink/{element}",BUCKET_NAME)

            # get recording date and time
            pattern = r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}'
            match = re.search(pattern, mp4_file_uri)
            if match:
                time_string = match.group()
                date_format = '%Y-%m-%d-%H-%M-%S'
                recording_datetime = datetime.strptime(time_string, date_format)
            else :
                recording_datetime = None
            
            logging.info(f"checking date time format : {recording_datetime}")

            columns = ['uri','conversation_duration','number_of_speakers','downloaded_date','date_transcribed','recording_datetime','word_count','call_id']
            data = [wav_file,conversation_duration,number_of_speakers,download_datetime,date_transcribed,recording_datetime,word_count,call_id]
            dict_metadata = {}
            for key, value in zip(columns, data):
                dict_metadata[key] = value
            logging.info (f"Metadata dictionary : {dict_metadata}")

            # yield [dict_metadata,list_dict_transcribe]

            if len(results) == 0:
                logging.info(f"*** Transcription error: Skipping entry to the metadata and transcription table ")
                # beam.pvalue.TaggedOutput('invalid', element)
            
            else:
                yield  [dict_metadata,list_dict_transcribe]

    def get_videofile_gcs_upload_time(self, file_path: str, bucket_name: str):

        logging.info(f"Getting upload date of file {file_path}")
        gscClient = storage.Client()
        bucket = gscClient.get_bucket(bucket_name)
        
        blob = bucket.get_blob(file_path)
        
        if blob is None:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        date_format = '%Y-%m-%d %H:%M:%S'
        return blob.time_created.strftime(date_format)
    
    def download_from_gcs(self, bucket_name: str, file_uri: str):
    
        root = path.dirname(path.abspath(__file__))
        #local_path = '/tmp/daily_features.csv'
        local_path = '/tmp/'
        local_path = path.join(root, local_path)

        # Defininf Storage client
        gscClient = storage.Client()
        logging.info("Storage Client created using default project: {}".format(gscClient.project))

        #Downloading daily features file from gcs
        bucket = gscClient.get_bucket(bucket_name)
        blob = bucket.get_blob(file_uri)
        file_path = local_path+'input.wav'

        blob.download_to_filename(file_path)
        logging.info("Downloading to environment:")
        logging.info("Name: {}".format(blob.id))
        logging.info("Size: {} bytes".format(blob.size))
        logging.info("Content type: {}".format(blob.content_type))

        return local_path,file_path

    def get_audio_metadata(self, output_file: str):
        with wave.open(output_file, "rb") as f:
            frames = f.getnframes()
            rate = f.getframerate()
        conversation_duration = frames // rate
        return conversation_duration
    
    def transribe_audio(self, gcs_uri: str,phrases: list) -> str:
        '''
        TO DO:
        Improve logging for each try catch block
        Based on the logs and the 4 sample files fix the code

        '''
        """Asynchronously transcribes the audio file specified by the gcs_uri.

        Args:
            gcs_uri: The Google Cloud Storage path to an audio file.

        Returns:
            The generated transcript from the audio file provided.
        """

        client = speech.SpeechClient()

        audio = speech.RecognitionAudio(uri=gcs_uri)
        try :
            config = speech.RecognitionConfig(
                # #encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
                # sample_rate_hertz=sample_rate,
                language_code="en-US",
                audio_channel_count = 2,
                enable_separate_recognition_per_channel = True,
                model = "latest_long",
                speech_contexts = phrases,
                diarization_config=speech.SpeakerDiarizationConfig(
                enable_speaker_diarization=True,
                min_speaker_count=2,  # Optional: Minimum number of speakers in the conversation
                max_speaker_count=3   # Optional: Maximum number of speakers in the conversation
                ),
                enable_automatic_punctuation=True,
                enable_word_time_offsets=True,
            )

            operation = client.long_running_recognize(config=config, audio=audio, timeout=1500)

        except Exception as e1 :
            logging.error(f"Exception caught for the config 2 audio channel count: {e1} ")
            try : 
                logging.info(f"This audio caused a error due to channel - channel count changed to 1: {gcs_uri}")
                config = speech.RecognitionConfig(
                    #encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
                    # sample_rate_hertz=sample_rate,
                    language_code="en-US",
                    audio_channel_count = 1,
                    enable_separate_recognition_per_channel = True,
                    model = "latest_long",
                    diarization_config=speech.SpeakerDiarizationConfig(
                    enable_speaker_diarization=True,
                    min_speaker_count=2,  # Optional: Minimum number of speakers in the conversation
                    max_speaker_count=3   # Optional: Maximum number of speakers in the conversation
                    ),
                    enable_word_time_offsets=True,
                )

                operation = client.long_running_recognize(config=config, audio=audio, timeout=1500)

            except Exception as e2 :
                logging.error(f"Exception caught for the config 1 audio channel count: {e2} ")
                try:
                    logging.info(f"This audio caused a error due to channel - seperate channel recognition is diabled: {gcs_uri}")
                    config = speech.RecognitionConfig(
                        #encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
                        # sample_rate_hertz=sample_rate,
                        language_code="en-US",
                        audio_channel_count = 2,
                        enable_separate_recognition_per_channel = False,
                        model = "latest_long",
                        diarization_config=speech.SpeakerDiarizationConfig(
                        enable_speaker_diarization=True,
                        min_speaker_count=2,  # Optional: Minimum number of speakers in the conversation
                        max_speaker_count=3   # Optional: Maximum number of speakers in the conversation
                        ),
                        enable_word_time_offsets=True,
                    )

                    operation = client.long_running_recognize(config=config, audio=audio, timeout=1500)
                
                except Exception as e:
                    logging.error(f"Exception caught for the config 2 audio channel count and last try block: {e1} ")
        
        logging.info("Waiting for operation to complete...")
        try : 
            response = operation.result(timeout=4000)
        except Exception as e:
            logging.info (f"Transcript response failure : {gcs_uri} **** exception : {e}")
            return []  # This should be checked
            '''
            If calls have silent moments the transcription API fails
            '''

        return list(response.results)

    def process_results_to_dict(self,response,uri,call_id):
        list_of_dicts = []
        my_dict = {}
        word_count = 0
        distinct_speakers = set()

        # response_flag is to capture uri of audios that has no content in it
        response_flag = 1
        for row_num,result in enumerate(response,start=1):
            response_flag = 0
            log_req = 0
            # logging.info (f"Printing result to find out of range list error: {result}")
            try :
                speaker = result.channel_tag
            except :
                log_req = 1
                speaker = None
            
            if speaker is not None:
                distinct_speakers.add(speaker)

            try :
                transcript = result.alternatives[0].transcript
            except :
                log_req = 1
                transcript = None

            if transcript is not None:
                word_count = word_count + len(transcript)

            try : 
                start_time = result.alternatives[0].words[0].start_time.seconds
            except :
                log_req = 1
                start_time = None

            try :
                end_time = result.result_end_time.seconds
            except :
                log_req = 1
                end_time = None

            try : 
                confidence = result.alternatives[0].confidence
            except : 
                log_req = 1
                confidence = None

            if log_req == 1:
                logging.warning(f"Transcript row {row_num} was nulled due to missing response results : {result}")

            my_dict = {
                'uri'        : uri , 
                'speaker'    : speaker,
                'transcript' : transcript,
                'start_time' : start_time,
                'end_time'   : end_time,
                'confidence' : confidence,
                'call_id'    : call_id
            }
            list_of_dicts.append(my_dict)
            # except :
            #     logging.warning(f"Transcript row {row_num} was skipped due to missing response results : {result}")
            #     continue
        
        if response_flag==1 :
            logging.warning(f"Audio is empty : {uri}")

            speaker = None
            transcript = None
            start_time = None
            end_time = None
            confidence = None

            my_dict = {
                'uri'        : uri , 
                'speaker'    : speaker,
                'transcript' : transcript,
                'start_time' : start_time,
                'end_time'   : end_time,
                'confidence' : confidence,
                'call_id'    : call_id
            }
            list_of_dicts.append(my_dict)

        return list_of_dicts,word_count,len(distinct_speakers)
        
class InterateTranscribe (beam.DoFn):

    def process(self, element):
         # Yield each dictionary
            for each_element in element:
                yield each_element
        

def run(argv=None):
    PROJECT_ID = "phone_tool"
    DATASET_ID = "phone_transcripts"
    PHRASES_TABLE = "speech_to_text_phrases_internal_table"
    ENV = "TEST" # PROD/TEST 

    if ENV == "PROD":
        print(" ENV variable set to PROD")
        gcs_file_path = "gs://call_center_recording/manifest_files/processed_manifest.csv"
        TABLE_ID_META = "call_recordings_metadata"
        TABLE_ID_TRANSCRIBE = "call_records_transcriptions"

    elif ENV == "TEST":
        print(" ENV variable set to TEST")
        gcs_file_path = "gs://call_center_recording/test_manifest_files/processed_manifest.csv"
        TABLE_ID_META = "call_recordings_metadata"
        TABLE_ID_TRANSCRIBE = "call_records_transcriptions"

    else:
        print("** ENV variable not set to TEST or PROD")
        print("** Exiting early **")
        return

    
    try:    
        bqclient = bigquery.Client(location="US")
        query = f"""
                SELECT 
                ARRAY_AGG(phrase) as phrases,
                COALESCE(weight, 20) AS boost
                FROM {PROJECT_ID}.{DATASET_ID}.{PHRASES_TABLE}
                where length(phrase) < 100 
                group by boost
                """
        
        query_job = bqclient.query(query,location="US")
        print(query_job)
        logging.info(f"query job is {query_job}")
        phrases_list = [dict(row) for row in query_job]
        
    except Exception as e:
        print(f"exception {e} occurred while reading phrases from BQ")
        phrases_list = []
    


    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        input_data = p | beam.io.ReadFromText(gcs_file_path)

        # input_data | 'Print' >> beam.Map(print)

        output_data = input_data | beam.ParDo(processAudioFile(phrases_list,ENV))
        # processed_data = (
        # input_data
        # | beam.ParDo(processAudioFile(phrases_list, ENV)).with_outputs('valid', 'invalid')
        # )


        # valid_data = processed_data.valid
        # invalid_data = processed_data.invalid
        # output_data = input_data | beam.ParDo(processAudioFile([]))

        result_metadata = output_data | "Choose Result Metadata" >> beam.Map(lambda x: x[0])

        result_transcript = output_data | "Choose Result Transcribe" >> beam.Map(lambda x: x[1])

        pardo_transcribe = result_transcript | "Iterate Transcribe data" >> beam.ParDo(InterateTranscribe())

        write_data_metadata = result_metadata | "Write To BigQuery Metadata" >> beam.io.WriteToBigQuery(
            table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID_META}',
            schema='uri:STRING, conversation_duration:INTEGER, number_of_speakers:INTEGER,downloaded_date:DATETIME, date_transcribed:DATETIME, recording_datetime:DATETIME, word_count:INTEGER, call_id:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        write_data_transcript = pardo_transcribe | "Write to BigQuery Transcribe" >> beam.io.WriteToBigQuery(
            table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID_TRANSCRIBE}',
            schema='uri:STRING, speaker:INTEGER, transcript:STRING, start_time:INTEGER, end_time:INTEGER, confidence:FLOAT, call_id:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__' : 
    print("Dataflow started")
    logging.getLogger().setLevel(logging.INFO)
    run()
    print("Dataflow process completed")