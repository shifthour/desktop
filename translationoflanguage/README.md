# File Translation Application

A web application for translating files between different languages.

## Features

- Upload various file formats (.txt, .pdf, .docx, .doc, .rtf, .html, .json, .xml, .csv)
- Select source and target languages
- Auto-detect source language option
- Download translated files
- Support for 15+ languages

## Setup

1. Install dependencies:
```bash
npm install
```

2. Start the server:
```bash
npm start
```

3. Open `index.html` in your browser or navigate to `http://localhost:3000`

## Using Google Translate API (Optional)

For real translations, set up a Google Translate API key:

1. Get an API key from [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the Cloud Translation API
3. Set the environment variable:
```bash
export GOOGLE_TRANSLATE_API_KEY=your_api_key_here
```

Without an API key, the app uses simulated translations for demonstration purposes.

## Usage

1. Click "Choose a file to upload" and select your file
2. Select the source language (or use Auto Detect)
3. Select the target language
4. Click "Translate File"
5. Once translation is complete, click "Download Translated File"