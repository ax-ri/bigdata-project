# [BigData] Study of popular songs around the world

<!-- <img align="right" src="assets/music-icon.png" width="20%"> -->

This project is part of the BigData (CY07) course at ENSTA Paris. It consists on working with a large amount of data and the tools seen in the course (as `spark`) to answer interesting questions in term of business. We have chosen to study musical popularity over the year and around the world. 

## Context
<!-- Context: what is the domain, what questions you are trying to answer. -->

Studying popularity of musics can be useful in term of: 
- **Geopolitics**: popularity of a song can be coupled with a socio-politics view of the region (can country be clustered by music or is music universal?)
- **Music industry**: discover the best criteria that make a music popular in order to create future hits 

For both point of view we created questions and try to answer them: 
- How does musical trends vary over the years?
- Can countries be clustered by similar music taste?
- Which musical characteristics (tempo, energy, etc.) give the best chance of success?
  
## Dataset
<!-- Dataset: source, size, format, structure, what it looks like -->

The dataset used for these studies is the Spotify Charts (All Audio Data) that is characterized by: 
<img align="right" src="assets/spotify-logo.png" width="20%">

- **Source** : Spotify Charts (All Audio Data) disponible sur [`kaggle.com`](https://www.kaggle.com/datasets/sunnykakar/spotify-charts-all-audio-data)
  - top 200 musics by region between 2017 and 2020 (given by Spotify)
  - enriched with metadata (Spotify API)
- **Format** : `CSV`
- **Size** : ~27 Go
- **Schema**: The dataset contains 29 columns that can be divided into three groups
  - characteristics to recognize a song (eg. *title*, *artist*)
  - characteristics to measure its impact on a region (eg. *streams*, *region*)
  - characteristics to describe a song (eg. audio features as *af_energy*)


  <details>

  <summary>See full schema</summary>

  ```
  root
  |-- id: long (nullable = true)
  |-- title: string (nullable = true)
  |-- rank: long (nullable = true)
  |-- date: date (nullable = true)
  |-- artist: string (nullable = true)
  |-- url: string (nullable = true)
  |-- region: string (nullable = true)
  |-- chart: string (nullable = true)
  |-- trend: string (nullable = true)
  |-- streams: long (nullable = true)
  |-- track_id: string (nullable = true)
  |-- album: string (nullable = true)
  |-- popularity: double (nullable = true)
  |-- duration_ms: double (nullable = true)
  |-- explicit: boolean (nullable = true)
  |-- release_date: date (nullable = true)
  |-- available_markets: string (nullable = true)
  |-- af_danceability: double (nullable = true) 	
  |-- af_energy: double (nullable = true) 	
  |-- af_key: double (nullable = true)			
  |-- af_loudness: double (nullable = true)
  |-- af_mode: boolean (nullable = true)		
  |-- af_speechiness: double (nullable = true)
  |-- af_acousticness: double (nullable = true)
  |-- af_instrumentalness: double (nullable = true)
  |-- af_liveness: double (nullable = true)
  |-- af_valence: double (nullable = true)			
  |-- af_tempo: double (nullable = true)				
  |-- af_time_signature: double (nullable = true)
  ```

  </details>
  
An extract of the dataset is presented in the table below (some columns are missing for a better comprehension): 

![Extract from the dataset (only 8 columns out of 29 are shown)](assets/dataset-sample.png "Extract from the dataset (only 8 columns out of 29 are shown)")


# to-do  
- Methodology: pipeline description, tools used, architecture choices.
- Results: key findings, presented with tables or charts.
- How to run: step-by-step instructions to reproduce your pipeline.
- Dependencies: list of all required software and libraries with versions