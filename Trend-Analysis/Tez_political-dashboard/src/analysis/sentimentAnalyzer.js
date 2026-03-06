import natural from 'natural';

class SentimentAnalyzer {
  constructor() {
    this.sentiment = new natural.SentimentAnalyzer('English', natural.PorterStemmer, 'afinn');
    
    this.positiveWords = [
      'development', 'progress', 'growth', 'success', 'achievement',
      'welfare', 'improvement', 'support', 'initiative', 'reform',
      'investment', 'employment', 'education', 'healthcare', 'infrastructure'
    ];
    
    this.negativeWords = [
      'corruption', 'scandal', 'failure', 'crisis', 'controversy',
      'protest', 'opposition', 'deficit', 'unemployment', 'inflation',
      'criticism', 'allegation', 'dispute', 'conflict', 'problem'
    ];
    
    this.partySpecificPositive = {
      ysrcp: ['welfare', 'social justice', 'farmer support', 'youth programs'],
      tdp: ['technology', 'infrastructure', 'investment', 'governance']
    };
    
    this.partySpecificNegative = {
      ysrcp: ['deficit', 'debt', 'opposition criticism'],
      tdp: ['alliance issues', 'rural disconnect']
    };
  }
  
  analyzeText(text) {
    const tokens = text.toLowerCase().split(/\s+/);
    const sentimentScore = this.sentiment.getSentiment(tokens);
    
    let positiveCount = 0;
    let negativeCount = 0;
    
    tokens.forEach(token => {
      if (this.positiveWords.includes(token)) positiveCount++;
      if (this.negativeWords.includes(token)) negativeCount++;
    });
    
    const customScore = (positiveCount - negativeCount) / Math.max(tokens.length, 1);
    const finalScore = (sentimentScore + customScore) / 2;
    
    return {
      score: finalScore,
      label: this.getLabel(finalScore),
      positive: positiveCount,
      negative: negativeCount,
      neutral: tokens.length - positiveCount - negativeCount
    };
  }
  
  getLabel(score) {
    if (score > 0.2) return 'positive';
    if (score < -0.2) return 'negative';
    return 'neutral';
  }
  
  analyzePartyNews(newsItems) {
    if (!newsItems || newsItems.length === 0) {
      return {
        overall: 0,
        label: 'neutral',
        distribution: { positive: 0, negative: 0, neutral: 0 }
      };
    }
    
    const sentiments = newsItems.map(item => {
      const analysis = this.analyzeText(item.title || '');
      return {
        ...item,
        sentiment: analysis.label,
        score: analysis.score
      };
    });
    
    const distribution = {
      positive: sentiments.filter(s => s.sentiment === 'positive').length,
      negative: sentiments.filter(s => s.sentiment === 'negative').length,
      neutral: sentiments.filter(s => s.sentiment === 'neutral').length
    };
    
    const avgScore = sentiments.reduce((sum, s) => sum + s.score, 0) / sentiments.length;
    
    return {
      overall: avgScore,
      label: this.getLabel(avgScore),
      distribution,
      items: sentiments
    };
  }
  
  async analyzeSentiment(data) {
    const ysrcpSentiment = this.analyzePartyNews(data.ysrcp.news);
    const tdpSentiment = this.analyzePartyNews(data.tdp.news);
    
    const ysrcpProsScore = this.calculateProsConsScore(data.ysrcp.pros, data.ysrcp.cons);
    const tdpProsScore = this.calculateProsConsScore(data.tdp.pros, data.tdp.cons);
    
    return {
      ysrcp: {
        news: ysrcpSentiment,
        prosConsBalance: ysrcpProsScore,
        overall: (ysrcpSentiment.overall + ysrcpProsScore) / 2,
        trend: this.calculateTrend(ysrcpSentiment.overall)
      },
      tdp: {
        news: tdpSentiment,
        prosConsBalance: tdpProsScore,
        overall: (tdpSentiment.overall + tdpProsScore) / 2,
        trend: this.calculateTrend(tdpSentiment.overall)
      },
      comparison: {
        leader: ysrcpSentiment.overall > tdpSentiment.overall ? 'YSRCP' : 'TDP',
        difference: Math.abs(ysrcpSentiment.overall - tdpSentiment.overall),
        sentiment: {
          ysrcp: this.getLabel(ysrcpSentiment.overall),
          tdp: this.getLabel(tdpSentiment.overall)
        }
      }
    };
  }
  
  calculateProsConsScore(pros, cons) {
    const prosWeight = pros ? pros.length * 0.2 : 0;
    const consWeight = cons ? cons.length * -0.15 : 0;
    return Math.max(-1, Math.min(1, prosWeight + consWeight));
  }
  
  calculateTrend(currentScore) {
    const random = Math.random();
    if (random > 0.7) return 'improving';
    if (random < 0.3) return 'declining';
    return 'stable';
  }
}

export default SentimentAnalyzer;