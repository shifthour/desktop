let uploadedFile = null;
let translatedContent = null;

const fileInput = document.getElementById('fileInput');
const fileName = document.getElementById('fileName');
const translateBtn = document.getElementById('translateBtn');
const downloadBtn = document.getElementById('downloadBtn');
const statusMessage = document.getElementById('statusMessage');
const progressBar = document.getElementById('progressBar');
const sourceLanguage = document.getElementById('sourceLanguage');
const targetLanguage = document.getElementById('targetLanguage');

fileInput.addEventListener('change', handleFileUpload);
translateBtn.addEventListener('click', translateFile);
downloadBtn.addEventListener('click', downloadTranslatedFile);

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file) {
        uploadedFile = file;
        fileName.textContent = file.name;
        translateBtn.disabled = false;
        downloadBtn.disabled = true;
        translatedContent = null;
        showStatus('File uploaded successfully!', 'success');
    }
}

async function translateFile() {
    if (!uploadedFile) {
        showStatus('Please upload a file first', 'error');
        return;
    }

    const sourceLang = sourceLanguage.value;
    const targetLang = targetLanguage.value;

    if (sourceLang === targetLang && sourceLang !== 'auto') {
        showStatus('Source and target languages cannot be the same', 'error');
        return;
    }

    translateBtn.disabled = true;
    progressBar.style.display = 'block';
    showStatus('Translating file...', 'info');

    try {
        const fileContent = await readFileContent(uploadedFile);
        
        const formData = new FormData();
        formData.append('content', fileContent);
        formData.append('sourceLang', sourceLang);
        formData.append('targetLang', targetLang);
        formData.append('fileName', uploadedFile.name);

        const response = await fetch('http://localhost:8080/translate', {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            throw new Error('Translation failed');
        }

        const result = await response.json();
        translatedContent = result.translatedContent;
        
        progressBar.style.display = 'none';
        showStatus('Translation completed successfully!', 'success');
        downloadBtn.disabled = false;
        translateBtn.disabled = false;

    } catch (error) {
        progressBar.style.display = 'none';
        showStatus('Translation failed: ' + error.message, 'error');
        translateBtn.disabled = false;
        console.error('Translation error:', error);
    }
}

function readFileContent(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (e) => resolve(e.target.result);
        reader.onerror = (e) => reject(e);
        reader.readAsText(file);
    });
}

function downloadTranslatedFile() {
    if (!translatedContent) {
        showStatus('No translated content available', 'error');
        return;
    }

    const originalName = uploadedFile.name;
    const nameParts = originalName.split('.');
    const extension = nameParts.pop();
    const baseName = nameParts.join('.');
    const newFileName = `${baseName}_translated_${targetLanguage.value}.${extension}`;

    const blob = new Blob([translatedContent], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = newFileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showStatus('File downloaded successfully!', 'success');
}

function showStatus(message, type) {
    statusMessage.textContent = message;
    statusMessage.className = `status-message ${type}`;
}