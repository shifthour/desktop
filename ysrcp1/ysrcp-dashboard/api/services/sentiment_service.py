"""
Sentiment Analysis Service
Uses VADER (Valence Aware Dictionary and sEntiment Reasoner)
- Free, no API needed
- Works well for social media text
"""

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from cachetools import TTLCache
from typing import Dict, List, Any, Optional
from config import YSRCP_KEYWORDS, TDP_KEYWORDS

# Cache for sentiment results
sentiment_cache = TTLCache(maxsize=500, ttl=3600)


class SentimentService:
    def __init__(self):
        self.vader = SentimentIntensityAnalyzer()

        # Add political context words to VADER
        self._add_political_lexicon()

    def _add_political_lexicon(self):
        """Add political terms to VADER's lexicon"""
        political_words = {
            # Positive political terms
            "welfare": 2.0,
            "development": 1.5,
            "progress": 1.5,
            "success": 2.0,
            "achievement": 2.0,
            "benefit": 1.5,
            "support": 1.0,
            "victory": 2.5,
            "growth": 1.5,

            # Negative political terms
            "corruption": -2.5,
            "scam": -3.0,
            "failure": -2.0,
            "protest": -1.0,
            "scandal": -2.5,
            "controversy": -1.5,
            "crisis": -2.0,
            "opposition": -0.5,

            # Telugu political terms (transliterated)
            "manchidi": 2.0,  # Good
            "chedda": -2.0,   # Bad
        }

        self.vader.lexicon.update(political_words)

    def analyze_text(self, text: str) -> Dict[str, Any]:
        """Analyze sentiment of a single text"""
        if not text:
            return {"compound": 0, "positive": 0, "negative": 0, "neutral": 1}

        # Use VADER for sentiment scores
        scores = self.vader.polarity_scores(text)

        # Classify sentiment
        compound = scores['compound']
        if compound >= 0.05:
            sentiment = "positive"
        elif compound <= -0.05:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        return {
            "sentiment": sentiment,
            "compound": compound,
            "positive": scores['pos'],
            "negative": scores['neg'],
            "neutral": scores['neu'],
            "confidence": abs(compound)
        }

    def analyze_batch(self, texts: List[str]) -> Dict[str, Any]:
        """Analyze sentiment of multiple texts"""
        if not texts:
            return {
                "overall": "neutral",
                "distribution": {"positive": 33, "negative": 33, "neutral": 34},
                "averageScore": 0
            }

        results = [self.analyze_text(text) for text in texts]

        # Calculate distribution
        positive_count = sum(1 for r in results if r['sentiment'] == 'positive')
        negative_count = sum(1 for r in results if r['sentiment'] == 'negative')
        neutral_count = sum(1 for r in results if r['sentiment'] == 'neutral')
        total = len(results)

        # Calculate average compound score
        avg_compound = sum(r['compound'] for r in results) / total if total > 0 else 0

        # Overall sentiment
        if avg_compound >= 0.05:
            overall = "positive"
        elif avg_compound <= -0.05:
            overall = "negative"
        else:
            overall = "neutral"

        return {
            "overall": overall,
            "distribution": {
                "positive": round((positive_count / total) * 100) if total > 0 else 33,
                "negative": round((negative_count / total) * 100) if total > 0 else 33,
                "neutral": round((neutral_count / total) * 100) if total > 0 else 34
            },
            "averageScore": round(avg_compound, 3),
            "totalAnalyzed": total
        }

    def classify_party_sentiment(self, text: str) -> Dict[str, Any]:
        """Classify sentiment specifically for YSRCP vs TDP"""
        text_lower = text.lower()

        # Check which party is mentioned
        ysrcp_mentioned = any(kw.lower() in text_lower for kw in YSRCP_KEYWORDS)
        tdp_mentioned = any(kw.lower() in text_lower for kw in TDP_KEYWORDS)

        sentiment = self.analyze_text(text)

        return {
            "text": text[:100] + "..." if len(text) > 100 else text,
            "sentiment": sentiment['sentiment'],
            "score": sentiment['compound'],
            "ysrcp_mentioned": ysrcp_mentioned,
            "tdp_mentioned": tdp_mentioned,
            "party_context": self._determine_party_context(text_lower, sentiment['sentiment'])
        }

    def _determine_party_context(self, text: str, sentiment: str) -> str:
        """Determine if sentiment is pro-YSRCP, pro-TDP, or neutral"""
        ysrcp_positive_indicators = ["ysrcp", "jagan", "welfare", "navaratnalu", "amma vodi"]
        tdp_positive_indicators = ["tdp", "chandrababu", "naidu", "amaravati"]

        ysrcp_score = sum(1 for ind in ysrcp_positive_indicators if ind in text)
        tdp_score = sum(1 for ind in tdp_positive_indicators if ind in text)

        if sentiment == "positive":
            if ysrcp_score > tdp_score:
                return "pro-ysrcp"
            elif tdp_score > ysrcp_score:
                return "pro-tdp"
        elif sentiment == "negative":
            if ysrcp_score > tdp_score:
                return "anti-ysrcp"
            elif tdp_score > ysrcp_score:
                return "anti-tdp"

        return "neutral"

    def get_sentiment_score(self, ysrcp_texts: List[str], tdp_texts: List[str]) -> Dict[str, Any]:
        """
        Calculate overall sentiment scores for both parties
        Returns scores on 0-100 scale for dashboard
        """
        cache_key = f"sentiment_score_{hash(str(ysrcp_texts[:5]))}"
        if cache_key in sentiment_cache:
            return sentiment_cache[cache_key]

        ysrcp_sentiment = self.analyze_batch(ysrcp_texts)
        tdp_sentiment = self.analyze_batch(tdp_texts)

        # Convert compound score (-1 to 1) to 0-100 scale
        # Formula: (compound + 1) / 2 * 100, then weighted by positive%
        ysrcp_score = self._calculate_party_score(ysrcp_sentiment)
        tdp_score = self._calculate_party_score(tdp_sentiment)

        result = {
            "ysrcp": {
                "score": ysrcp_score,
                "sentiment": ysrcp_sentiment['distribution'],
                "overall": ysrcp_sentiment['overall']
            },
            "tdp": {
                "score": tdp_score,
                "sentiment": tdp_sentiment['distribution'],
                "overall": tdp_sentiment['overall']
            },
            "comparison": {
                "leader": "ysrcp" if ysrcp_score > tdp_score else "tdp",
                "difference": abs(ysrcp_score - tdp_score)
            }
        }

        sentiment_cache[cache_key] = result
        return result

    def _calculate_party_score(self, sentiment_data: Dict) -> int:
        """Calculate a 0-100 score based on sentiment distribution"""
        pos = sentiment_data['distribution']['positive']
        neg = sentiment_data['distribution']['negative']

        # Score = positive% - (negative% * 0.5) + 50 (baseline)
        # This gives higher weight to positive mentions
        score = pos - (neg * 0.5) + 30

        # Clamp between 0-100
        return max(0, min(100, int(score)))

    def extract_key_phrases(self, texts: List[str], top_n: int = 15) -> List[Dict[str, Any]]:
        """Extract key phrases/topics from texts using TextBlob"""
        all_phrases = {}

        for text in texts:
            try:
                blob = TextBlob(text)
                # Extract noun phrases
                for phrase in blob.noun_phrases:
                    phrase_lower = phrase.lower()
                    if len(phrase_lower) > 3:  # Filter short phrases
                        all_phrases[phrase_lower] = all_phrases.get(phrase_lower, 0) + 1
            except:
                continue

        # Sort by frequency
        sorted_phrases = sorted(all_phrases.items(), key=lambda x: x[1], reverse=True)

        return [
            {"phrase": phrase, "count": count, "sentiment": self.analyze_text(phrase)['sentiment']}
            for phrase, count in sorted_phrases[:top_n]
        ]


# Singleton instance
sentiment_service = SentimentService()
