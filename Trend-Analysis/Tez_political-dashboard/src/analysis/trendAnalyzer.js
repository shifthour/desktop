class TrendAnalyzer {
  constructor() {
    this.historicalData = {
      ysrcp: [],
      tdp: []
    };
    
    this.metrics = [
      'popularity',
      'mediaPresence',
      'socialMedia',
      'publicSentiment'
    ];
  }
  
  generateHistoricalData(party) {
    const data = [];
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const currentMonth = new Date().getMonth();
    
    for (let i = 0; i < 12; i++) {
      const monthIndex = (currentMonth - 11 + i + 12) % 12;
      const baseValue = party === 'ysrcp' ? 42 : 45;
      const variation = Math.sin(i * 0.5) * 10 + Math.random() * 5;
      
      data.push({
        month: months[monthIndex],
        popularity: baseValue + variation,
        mediaPresence: 60 + Math.random() * 20,
        socialMedia: 55 + Math.random() * 25,
        events: Math.floor(Math.random() * 10) + 5
      });
    }
    
    return data;
  }
  
  calculateMovingAverage(data, period = 3) {
    const result = [];
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) {
        result.push(data[i]);
      } else {
        const sum = data.slice(i - period + 1, i + 1).reduce((a, b) => a + b, 0);
        result.push(sum / period);
      }
    }
    return result;
  }
  
  detectTrendDirection(data) {
    if (data.length < 2) return 'stable';
    
    const recentAvg = data.slice(-3).reduce((a, b) => a + b, 0) / Math.min(3, data.slice(-3).length);
    const previousAvg = data.slice(-6, -3).reduce((a, b) => a + b, 0) / Math.min(3, data.slice(-6, -3).length);
    
    const change = ((recentAvg - previousAvg) / previousAvg) * 100;
    
    if (change > 5) return 'upward';
    if (change < -5) return 'downward';
    return 'stable';
  }
  
  analyzeTrends(data) {
    const ysrcpHistory = this.generateHistoricalData('ysrcp');
    const tdpHistory = this.generateHistoricalData('tdp');
    
    const ysrcpPopularity = ysrcpHistory.map(d => d.popularity);
    const tdpPopularity = tdpHistory.map(d => d.popularity);
    
    const ysrcpMA = this.calculateMovingAverage(ysrcpPopularity);
    const tdpMA = this.calculateMovingAverage(tdpPopularity);
    
    return {
      ysrcp: {
        historical: ysrcpHistory,
        currentMetrics: data.ysrcp.metrics,
        movingAverage: ysrcpMA,
        trend: this.detectTrendDirection(ysrcpPopularity),
        forecast: this.generateForecast(ysrcpPopularity),
        keyInsights: this.generateInsights('YSRCP', data.ysrcp)
      },
      tdp: {
        historical: tdpHistory,
        currentMetrics: data.tdp.metrics,
        movingAverage: tdpMA,
        trend: this.detectTrendDirection(tdpPopularity),
        forecast: this.generateForecast(tdpPopularity),
        keyInsights: this.generateInsights('TDP', data.tdp)
      },
      comparison: {
        popularityGap: Math.abs(data.ysrcp.metrics.popularity - data.tdp.metrics.popularity),
        leadingParty: data.ysrcp.metrics.popularity > data.tdp.metrics.popularity ? 'YSRCP' : 'TDP',
        convergenceTrend: this.calculateConvergence(ysrcpPopularity, tdpPopularity),
        competitiveIndex: this.calculateCompetitiveIndex(data)
      }
    };
  }
  
  generateForecast(historicalData, periods = 3) {
    const trend = this.detectTrendDirection(historicalData);
    const lastValue = historicalData[historicalData.length - 1];
    const forecast = [];
    
    for (let i = 1; i <= periods; i++) {
      let change = 0;
      if (trend === 'upward') change = Math.random() * 5;
      else if (trend === 'downward') change = -Math.random() * 5;
      else change = (Math.random() - 0.5) * 3;
      
      forecast.push(Math.max(0, Math.min(100, lastValue + change * i)));
    }
    
    return forecast;
  }
  
  generateInsights(party, partyData) {
    const insights = [];
    
    if (partyData.metrics.popularity > 50) {
      insights.push(`${party} showing strong public support with ${partyData.metrics.popularity}% approval`);
    }
    
    if (partyData.metrics.socialMedia > 65) {
      insights.push(`High social media engagement indicates strong youth connect`);
    }
    
    if (partyData.metrics.mediaPresence > 70) {
      insights.push(`Dominant media presence helping shape public narrative`);
    }
    
    if (partyData.pros && partyData.pros.length > partyData.cons.length) {
      insights.push(`Positive perception outweighs challenges`);
    }
    
    if (insights.length === 0) {
      insights.push(`Maintaining steady position in political landscape`);
    }
    
    return insights;
  }
  
  calculateConvergence(data1, data2) {
    const recent1 = data1.slice(-3).reduce((a, b) => a + b, 0) / 3;
    const recent2 = data2.slice(-3).reduce((a, b) => a + b, 0) / 3;
    const gap = Math.abs(recent1 - recent2);
    
    const previous1 = data1.slice(-6, -3).reduce((a, b) => a + b, 0) / 3;
    const previous2 = data2.slice(-6, -3).reduce((a, b) => a + b, 0) / 3;
    const previousGap = Math.abs(previous1 - previous2);
    
    if (gap < previousGap) return 'converging';
    if (gap > previousGap) return 'diverging';
    return 'parallel';
  }
  
  calculateCompetitiveIndex(data) {
    const ysrcpScore = Object.values(data.ysrcp.metrics).reduce((a, b) => a + b, 0) / 4;
    const tdpScore = Object.values(data.tdp.metrics).reduce((a, b) => a + b, 0) / 4;
    
    const competitiveness = 100 - Math.abs(ysrcpScore - tdpScore);
    
    if (competitiveness > 90) return 'Very High';
    if (competitiveness > 75) return 'High';
    if (competitiveness > 60) return 'Moderate';
    if (competitiveness > 45) return 'Low';
    return 'Very Low';
  }
}

export default TrendAnalyzer;