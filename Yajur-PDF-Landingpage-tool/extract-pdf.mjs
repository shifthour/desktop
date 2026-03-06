import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';

const doc = await getDocument('/Users/safestorage/Downloads/YKS_MCM SRS_RP_012026_V9.pdf').promise;
console.log('Total pages:', doc.numPages);

let fullText = '';
for (let i = 1; i <= Math.min(doc.numPages, 20); i++) {
  const page = await doc.getPage(i);
  const content = await page.getTextContent();
  const pageText = content.items.map(item => item.str).join(' ');
  fullText += `\n--- PAGE ${i} ---\n${pageText}`;
}
console.log(fullText.substring(0, 15000));
