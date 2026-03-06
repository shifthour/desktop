const express = require('express');
const multer = require('multer');
const cors = require('cors');
const axios = require('axios');

const app = express();
const upload = multer();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('.'));

app.post('/translate', upload.none(), async (req, res) => {
    try {
        const { content, sourceLang, targetLang } = req.body;

        if (!content || !targetLang) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const translatedContent = await translateText(content, sourceLang, targetLang);

        res.json({
            translatedContent,
            success: true
        });

    } catch (error) {
        console.error('Translation error:', error);
        res.status(500).json({
            error: 'Translation failed',
            message: error.message
        });
    }
});

async function translateText(text, sourceLang, targetLang) {
    const MAX_CHUNK_SIZE = 450;
    
    if (text.length <= MAX_CHUNK_SIZE) {
        return translateChunk(text, sourceLang, targetLang);
    }
    
    const chunks = splitTextIntoChunks(text, MAX_CHUNK_SIZE);
    const translatedChunks = [];
    
    for (const chunk of chunks) {
        const translated = await translateChunk(chunk, sourceLang, targetLang);
        translatedChunks.push(translated);
        await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    return translatedChunks.join('');
}

function splitTextIntoChunks(text, maxSize) {
    const chunks = [];
    const sentences = text.split(/([.!?\n]+)/);
    let currentChunk = '';
    
    for (let i = 0; i < sentences.length; i++) {
        const sentence = sentences[i];
        
        if ((currentChunk + sentence).length <= maxSize) {
            currentChunk += sentence;
        } else {
            if (currentChunk) {
                chunks.push(currentChunk);
            }
            
            if (sentence.length > maxSize) {
                const words = sentence.split(' ');
                let wordChunk = '';
                
                for (const word of words) {
                    if ((wordChunk + ' ' + word).length <= maxSize) {
                        wordChunk += (wordChunk ? ' ' : '') + word;
                    } else {
                        if (wordChunk) chunks.push(wordChunk);
                        wordChunk = word;
                    }
                }
                if (wordChunk) currentChunk = wordChunk;
            } else {
                currentChunk = sentence;
            }
        }
    }
    
    if (currentChunk) {
        chunks.push(currentChunk);
    }
    
    return chunks;
}

async function translateChunk(text, sourceLang, targetLang) {
    try {
        const response = await axios.post('https://libretranslate.com/translate', {
            q: text,
            source: sourceLang === 'auto' ? 'auto' : sourceLang,
            target: targetLang,
            format: 'text'
        }, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        return response.data.translatedText;
    } catch (error) {
        console.error('LibreTranslate API error:', error);
        
        try {
            const response = await axios.get('https://api.mymemory.translated.net/get', {
                params: {
                    q: text,
                    langpair: sourceLang === 'auto' ? `en|${targetLang}` : `${sourceLang}|${targetLang}`
                }
            });
            
            if (response.data && response.data.responseData) {
                return response.data.responseData.translatedText;
            }
        } catch (fallbackError) {
            console.error('MyMemory API error:', fallbackError);
        }
        
        return text;
    }
}

function simulateTranslation(text, targetLang) {
    const languageNames = {
        'en': 'English',
        'es': 'Spanish',
        'fr': 'French',
        'de': 'German',
        'it': 'Italian',
        'pt': 'Portuguese',
        'ru': 'Russian',
        'zh': 'Chinese',
        'ja': 'Japanese',
        'ko': 'Korean',
        'ar': 'Arabic',
        'hi': 'Hindi',
        'nl': 'Dutch',
        'pl': 'Polish',
        'tr': 'Turkish'
    };

    return `[Translated to ${languageNames[targetLang] || targetLang}]\n\n${text}\n\n[Note: This is a simulated translation. For real translations, set up Google Translate API key in environment variables.]`;
}

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Translation server running on http://localhost:${PORT}`);
    console.log('To use real translation, set GOOGLE_TRANSLATE_API_KEY environment variable');
});