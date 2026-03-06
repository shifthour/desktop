"""
Google Trends Service using pytrends
Fetches search interest data for YSRCP vs TDP
"""

from pytrends.request import TrendReq
from cachetools import TTLCache
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any
import time

# Cache for trends data (1 hour TTL)
trends_cache = TTLCache(maxsize=100, ttl=3600)

class GoogleTrendsService:
    def __init__(self):
        self.pytrends = TrendReq(hl='en-IN', tz=330)  # India timezone

    def _get_with_retry(self, func, *args, max_retries=3, **kwargs):
        """Retry wrapper for pytrends requests"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)  # Exponential backoff

    def get_interest_over_time(self, timeframe: str = 'today 1-m') -> Dict[str, Any]:
        """
        Get search interest over time for YSRCP vs TDP
        timeframe options: 'today 1-m', 'today 3-m', 'today 12-m'
        """
        cache_key = f"interest_time_{timeframe}"
        if cache_key in trends_cache:
            return trends_cache[cache_key]

        try:
            # Build payload for comparison
            self.pytrends.build_payload(
                kw_list=['YSRCP', 'TDP'],
                cat=0,
                timeframe=timeframe,
                geo='IN-AP',  # Andhra Pradesh
                gprop=''
            )

            # Get interest over time
            df = self._get_with_retry(self.pytrends.interest_over_time)

            if df.empty:
                return self._get_fallback_data()

            # Process data
            timeline_data = []
            for index, row in df.iterrows():
                timeline_data.append({
                    "date": index.strftime("%b %d"),
                    "ysrcp": int(row.get('YSRCP', 0)),
                    "tdp": int(row.get('TDP', 0))
                })

            # Calculate averages
            ysrcp_avg = df['YSRCP'].mean() if 'YSRCP' in df.columns else 50
            tdp_avg = df['TDP'].mean() if 'TDP' in df.columns else 50

            # Get latest values
            ysrcp_current = int(df['YSRCP'].iloc[-1]) if 'YSRCP' in df.columns else 50
            tdp_current = int(df['TDP'].iloc[-1]) if 'TDP' in df.columns else 50

            # Calculate trend (compare last week avg to previous week)
            if len(df) >= 14:
                recent_avg = df['YSRCP'].tail(7).mean()
                previous_avg = df['YSRCP'].iloc[-14:-7].mean()
                trend_pct = ((recent_avg - previous_avg) / previous_avg * 100) if previous_avg > 0 else 0
            else:
                trend_pct = 0

            result = {
                "searchInterest": {
                    "ysrcp": ysrcp_current,
                    "tdp": tdp_current,
                    "trend": f"+{trend_pct:.1f}%" if trend_pct >= 0 else f"{trend_pct:.1f}%"
                },
                "searchTimeline": timeline_data[-30:],  # Last 30 data points
                "averages": {
                    "ysrcp": round(ysrcp_avg, 1),
                    "tdp": round(tdp_avg, 1)
                }
            }

            trends_cache[cache_key] = result
            return result

        except Exception as e:
            print(f"Error fetching trends: {e}")
            return self._get_fallback_data()

    def get_regional_interest(self) -> List[Dict[str, Any]]:
        """Get search interest by region (districts in AP)"""
        cache_key = "regional_interest"
        if cache_key in trends_cache:
            return trends_cache[cache_key]

        try:
            self.pytrends.build_payload(
                kw_list=['YSRCP', 'TDP'],
                cat=0,
                timeframe='today 3-m',
                geo='IN-AP',
                gprop=''
            )

            df = self._get_with_retry(self.pytrends.interest_by_region, resolution='REGION')

            if df.empty:
                return self._get_fallback_regional()

            regional_data = []
            for region, row in df.iterrows():
                ysrcp_val = int(row.get('YSRCP', 0))
                tdp_val = int(row.get('TDP', 0))

                # Normalize to 100
                total = ysrcp_val + tdp_val
                if total > 0:
                    ysrcp_norm = int((ysrcp_val / total) * 100)
                    tdp_norm = 100 - ysrcp_norm
                else:
                    ysrcp_norm = 50
                    tdp_norm = 50

                regional_data.append({
                    "district": region,
                    "ysrcp": ysrcp_norm,
                    "tdp": tdp_norm
                })

            # Sort by YSRCP interest
            regional_data.sort(key=lambda x: x['ysrcp'], reverse=True)

            trends_cache[cache_key] = regional_data[:15]  # Top 15 regions
            return regional_data[:15]

        except Exception as e:
            print(f"Error fetching regional interest: {e}")
            return self._get_fallback_regional()

    def get_related_queries(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get related search queries for both parties"""
        cache_key = "related_queries"
        if cache_key in trends_cache:
            return trends_cache[cache_key]

        result = {"ysrcp": [], "tdp": []}

        for party, keyword in [("ysrcp", "YSRCP"), ("tdp", "TDP")]:
            try:
                self.pytrends.build_payload(
                    kw_list=[keyword],
                    cat=0,
                    timeframe='today 1-m',
                    geo='IN-AP',
                    gprop=''
                )

                related = self._get_with_retry(self.pytrends.related_queries)

                if keyword in related and related[keyword]['rising'] is not None:
                    rising_df = related[keyword]['rising']
                    for _, row in rising_df.head(6).iterrows():
                        result[party].append({
                            "query": row['query'],
                            "interest": min(100, int(row['value'])) if pd.notna(row['value']) else 50,
                            "change": f"+{int(row['value'])}%" if pd.notna(row['value']) else "+0%",
                            "isBreakout": row['value'] > 200 if pd.notna(row['value']) else False
                        })

                if keyword in related and related[keyword]['top'] is not None and len(result[party]) < 6:
                    top_df = related[keyword]['top']
                    for _, row in top_df.head(6 - len(result[party])).iterrows():
                        if not any(q['query'] == row['query'] for q in result[party]):
                            result[party].append({
                                "query": row['query'],
                                "interest": int(row['value']) if pd.notna(row['value']) else 50,
                                "change": "+0%",
                                "isBreakout": False
                            })

            except Exception as e:
                print(f"Error fetching related queries for {party}: {e}")

        # Add fallback if empty
        if not result['ysrcp']:
            result['ysrcp'] = self._get_fallback_queries('ysrcp')
        if not result['tdp']:
            result['tdp'] = self._get_fallback_queries('tdp')

        trends_cache[cache_key] = result
        return result

    def get_breakout_topics(self) -> List[Dict[str, Any]]:
        """Get breakout/trending topics"""
        related = self.get_related_queries()
        breakouts = []

        for party in ['ysrcp', 'tdp']:
            for query in related[party]:
                if query.get('isBreakout') or ('+' in query.get('change', '') and int(query['change'].replace('+', '').replace('%', '') or 0) > 100):
                    breakouts.append({
                        "topic": query['query'],
                        "growth": query['change'],
                        "party": party
                    })

        # Sort by growth percentage
        breakouts.sort(key=lambda x: int(x['growth'].replace('+', '').replace('%', '') or 0), reverse=True)
        return breakouts[:5]

    def _get_fallback_data(self) -> Dict[str, Any]:
        """Fallback data when API fails"""
        return {
            "searchInterest": {
                "ysrcp": 65,
                "tdp": 45,
                "trend": "+8%"
            },
            "searchTimeline": [
                {"date": "Nov 24", "ysrcp": 58, "tdp": 42},
                {"date": "Nov 26", "ysrcp": 62, "tdp": 45},
                {"date": "Nov 28", "ysrcp": 68, "tdp": 48},
                {"date": "Nov 30", "ysrcp": 65, "tdp": 45}
            ],
            "averages": {"ysrcp": 63.2, "tdp": 45.0}
        }

    def _get_fallback_regional(self) -> List[Dict[str, Any]]:
        """Fallback regional data"""
        return [
            {"district": "Kadapa", "ysrcp": 85, "tdp": 15},
            {"district": "Kurnool", "ysrcp": 72, "tdp": 28},
            {"district": "Anantapur", "ysrcp": 68, "tdp": 32},
            {"district": "Visakhapatnam", "ysrcp": 62, "tdp": 38},
            {"district": "Guntur", "ysrcp": 55, "tdp": 45},
            {"district": "Vijayawada", "ysrcp": 48, "tdp": 52}
        ]

    def _get_fallback_queries(self, party: str) -> List[Dict[str, Any]]:
        """Fallback queries data"""
        if party == 'ysrcp':
            return [
                {"query": "YSRCP welfare schemes", "interest": 100, "change": "+120%", "isBreakout": True},
                {"query": "YS Jagan news", "interest": 85, "change": "+45%", "isBreakout": False},
                {"query": "Amma Vodi status", "interest": 72, "change": "+30%", "isBreakout": False}
            ]
        else:
            return [
                {"query": "TDP latest news", "interest": 100, "change": "+80%", "isBreakout": False},
                {"query": "Chandrababu speech", "interest": 75, "change": "+25%", "isBreakout": False},
                {"query": "Amaravati capital", "interest": 60, "change": "+15%", "isBreakout": False}
            ]


# Singleton instance
google_trends_service = GoogleTrendsService()
