import { Cloud, Wifi, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const WordCloud = () => {
  const { trendingHashtags, googleTrends, isLive } = useDashboard();

  // Build word cloud data from real hashtags and Google Trends queries
  const buildWordCloudData = () => {
    const words = [];
    const seenWords = new Set();

    // Add trending hashtags
    if (trendingHashtags && trendingHashtags.length > 0) {
      trendingHashtags.forEach(tag => {
        const text = tag.tag?.replace('#', '') || '';
        if (text && !seenWords.has(text.toLowerCase())) {
          seenWords.add(text.toLowerCase());
          words.push({
            text: text,
            value: tag.count || tag.engagement || 50,
            party: tag.party
          });
        }
      });
    }

    // Add Google Trends related queries
    const ysrcpQueries = googleTrends?.queries?.ysrcp || [];
    const tdpQueries = googleTrends?.queries?.tdp || [];

    ysrcpQueries.slice(0, 5).forEach(q => {
      const text = q.query || '';
      if (text && !seenWords.has(text.toLowerCase())) {
        seenWords.add(text.toLowerCase());
        words.push({
          text: text,
          value: q.interest || 50,
          party: 'ysrcp'
        });
      }
    });

    tdpQueries.slice(0, 5).forEach(q => {
      const text = q.query || '';
      if (text && !seenWords.has(text.toLowerCase())) {
        seenWords.add(text.toLowerCase());
        words.push({
          text: text,
          value: q.interest || 50,
          party: 'tdp'
        });
      }
    });

    // Add Google Trends breakout topics
    const breakout = googleTrends?.breakout || [];
    breakout.slice(0, 5).forEach(topic => {
      const text = topic.topic || topic.query || '';
      if (text && !seenWords.has(text.toLowerCase())) {
        seenWords.add(text.toLowerCase());
        words.push({
          text: text,
          value: topic.interest || 80,
          party: topic.party || 'general'
        });
      }
    });

    return words;
  };

  const realWordCloudData = buildWordCloudData();
  const hasRealData = realWordCloudData.length > 0;

  // Show loading if no data
  if (!hasRealData) {
    return (
      <div className="card p-6 h-full">
        <div className="flex items-center gap-2 mb-4">
          <Cloud size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Trending Topics</h2>
        </div>
        <div className="flex items-center justify-center h-[200px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  const wordCloudData = realWordCloudData;

  const maxValue = Math.max(...wordCloudData.map(w => w.value), 1);
  const minValue = Math.min(...wordCloudData.map(w => w.value), 0);

  const getSize = (value) => {
    const normalized = maxValue > minValue ? (value - minValue) / (maxValue - minValue) : 0.5;
    return 14 + normalized * 20; // Font size between 14px and 34px
  };

  const getOpacity = (value) => {
    const normalized = maxValue > minValue ? (value - minValue) / (maxValue - minValue) : 0.5;
    return 0.5 + normalized * 0.5; // Opacity between 0.5 and 1
  };

  const getColor = (word, index) => {
    if (word.party === 'ysrcp') return '#3399FF';
    if (word.party === 'tdp') return '#FFD700';
    return index % 3 === 0 ? '#3399FF' : index % 3 === 1 ? '#FFD700' : '#e4e4e7';
  };

  // Shuffle array for random positioning effect
  const shuffledData = [...wordCloudData].sort(() => Math.random() - 0.5);

  return (
    <div className="card p-6 h-full">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Cloud size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Trending Topics</h2>
          {hasRealData && isLive && (
            <span className="flex items-center gap-1 text-xs text-green-500 bg-green-500/10 px-2 py-0.5 rounded-full">
              <Wifi size={10} />
              LIVE
            </span>
          )}
        </div>
        <span className="text-xs text-dashboard-textMuted">What people are talking about</span>
      </div>

      <div className="flex flex-wrap items-center justify-center gap-3 py-6">
        {shuffledData.map((word, index) => (
          <span
            key={word.text}
            className="px-2 py-1 rounded-lg bg-dashboard-bg hover:bg-ysrcp-primary/20 transition-colors cursor-pointer"
            style={{
              fontSize: `${getSize(word.value)}px`,
              opacity: getOpacity(word.value),
              color: getColor(word, index)
            }}
          >
            {word.text}
          </span>
        ))}
      </div>

      <div className="pt-4 border-t border-dashboard-border text-center">
        <p className="text-xs text-dashboard-textMuted">
          Size indicates mention frequency | Hover to interact
        </p>
      </div>
    </div>
  );
};

export default WordCloud;
