import React from 'react';
import { TrainLog, CorridorStats } from './types';
import { parseTrainLogs, computeCorridorStats } from './lib/dataParser';
import { Header } from './components/Header';
import { CorridorOverview } from './components/CorridorOverview';
import { TrainDetailView } from './components/TrainDetailView';
import { TrainComparisonView } from './components/TrainComparisonView';
import { Train, RefreshCw, AlertCircle } from 'lucide-react';
import rawCsvData from './data/hamburg_cph_trains.csv?raw';

export default function App() {
  const [allLogs, setAllLogs] = React.useState<TrainLog[]>(() => {
    try {
      if (rawCsvData) {
        return parseTrainLogs(rawCsvData);
      }
    } catch (e) {
      console.error('Error parsing embedded CSV data:', e);
    }
    return [];
  });
  const [loading, setLoading] = React.useState<boolean>(allLogs.length === 0);
  const [error, setError] = React.useState<string | null>(null);

  const [activeTab, setActiveTab] = React.useState<'overview' | 'detail' | 'compare'>('overview');
  const [selectedTrainId, setSelectedTrainId] = React.useState<string>('397');

  // Async backup fetch for latest CSV if available
  React.useEffect(() => {
    if (allLogs.length > 0) return; // Already loaded via embedded raw bundle

    fetch(`https://raw.githubusercontent.com/lucasvduin/talgo-tracker/main/hamburg_cph_trains.csv?t=${Date.now()}`)
          .then((res) => {
                  if (!res.ok) throw new Error(`HTTP ${res.status}: Failed to load CSV data`);
                        
                        
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}: Failed to load CSV data`);
        return res.text();
      })
      .then((csvText) => {
        const parsed = parseTrainLogs(csvText);
        setAllLogs(parsed);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Error loading CSV via fetch:', err);
        // If we have rawCsvData, fallback to it
        if (rawCsvData) {
          const parsed = parseTrainLogs(rawCsvData);
          setAllLogs(parsed);
          setError(null);
        } else {
          setError(err.message || 'Failed to load historical train logs.');
        }
        setLoading(false);
      });
  }, [allLogs.length]);

  // Compute corridor stats & unique train IDs
  const corridorStats: CorridorStats | null = React.useMemo(() => {
    if (allLogs.length === 0) return null;
    return computeCorridorStats(allLogs);
  }, [allLogs]);

  // All unique train IDs sorted numerically
  const allTrainIds = React.useMemo(() => {
    const ids = new Set<string>();
    allLogs.forEach((l) => {
      if (l.trainId) ids.add(l.trainId);
    });
    return Array.from(ids).sort((a, b) => {
      const na = parseInt(a, 10);
      const nb = parseInt(b, 10);
      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return a.localeCompare(b);
    });
  }, [allLogs]);

  const handleSelectTrain = (id: string) => {
    setSelectedTrainId(id);
    setActiveTab('detail');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col items-center justify-center p-6 space-y-4">
        <div className="p-4 rounded-2xl bg-slate-900 border border-slate-800 shadow-2xl flex items-center gap-3 animate-pulse">
          <Train className="h-8 w-8 text-rose-500 animate-bounce" />
          <div>
            <h2 className="text-lg font-bold text-white">Loading Talgo Tracker Dataset...</h2>
            <p className="text-xs text-slate-400">Parsing 1,900+ historical train records</p>
          </div>
        </div>
      </div>
    );
  }

  if (error || !corridorStats) {
    return (
      <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col items-center justify-center p-6">
        <div className="max-w-md w-full bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl text-center space-y-4">
          <AlertCircle className="h-10 w-10 text-rose-500 mx-auto" />
          <h2 className="text-xl font-bold text-white">Error Loading Data</h2>
          <p className="text-xs text-slate-400">{error || 'No train logs available.'}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-rose-600 text-white font-semibold text-xs rounded-xl shadow-lg hover:bg-rose-500"
          >
            Retry Loading
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 font-sans selection:bg-rose-500 selection:text-white pb-16">
      {/* Header Bar */}
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        selectedTrainId={selectedTrainId}
        setSelectedTrainId={handleSelectTrain}
        allTrainIds={allTrainIds}
        totalLogsCount={allLogs.length}
      />

      {/* Main Container */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-6">
        {activeTab === 'overview' && (
          <CorridorOverview
            corridorStats={corridorStats}
            allLogs={allLogs}
            allTrainIds={allTrainIds}
            onSelectTrain={handleSelectTrain}
          />
        )}

        {activeTab === 'detail' && (
          <TrainDetailView
            selectedTrainId={selectedTrainId}
            onSelectTrain={setSelectedTrainId}
            allTrainIds={allTrainIds}
            allLogs={allLogs}
          />
        )}

        {activeTab === 'compare' && (
          <TrainComparisonView
            allTrainIds={allTrainIds}
            allLogs={allLogs}
            defaultTrainA={selectedTrainId}
            defaultTrainB={selectedTrainId === '396' ? '397' : '396'}
          />
        )}
      </main>

      {/* Footer */}
      <footer className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 mt-16 pt-8 border-t border-slate-900 text-center text-xs text-slate-400">
        <p>
          🚆 <strong className="text-slate-300">Talgo Tracker Analytics Engine</strong> •
          Tracking DSB Talgo 230, Railjet, IC3, & German IC international services passing Padborg.
        </p>
      </footer>
    </div>
  );
}
