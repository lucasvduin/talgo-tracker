import React from 'react';
import { Train, BarChart3, Layers, GitCompare, Search, ShieldAlert, Sparkles } from 'lucide-react';

interface HeaderProps {
  activeTab: 'overview' | 'detail' | 'compare';
  setActiveTab: (tab: 'overview' | 'detail' | 'compare') => void;
  selectedTrainId: string;
  setSelectedTrainId: (id: string) => void;
  allTrainIds: string[];
  totalLogsCount: number;
}

export const Header: React.FC<HeaderProps> = ({
  activeTab,
  setActiveTab,
  selectedTrainId,
  setSelectedTrainId,
  allTrainIds,
  totalLogsCount,
}) => {
  const [searchInput, setSearchInput] = React.useState('');
  const [isSearching, setIsSearching] = React.useState(false);

  const filteredTrainIds = React.useMemo(() => {
    if (!searchInput.trim()) return allTrainIds.slice(0, 10);
    return allTrainIds
      .filter((id) => id.toLowerCase().includes(searchInput.trim().toLowerCase()))
      .slice(0, 10);
  }, [allTrainIds, searchInput]);

  const handleSelectTrain = (id: string) => {
    setSelectedTrainId(id);
    setActiveTab('detail');
    setIsSearching(false);
    setSearchInput('');
  };

  return (
    <header className="bg-slate-900 border-b border-slate-800 sticky top-0 z-50 backdrop-blur-md bg-slate-900/90">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3.5">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          
          {/* Logo & Subtitle */}
          <div className="flex items-center gap-3">
            <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-rose-500 to-red-700 flex items-center justify-center text-white shadow-lg shadow-rose-900/30 ring-1 ring-white/20">
              <Train className="h-5 w-5" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-xl font-bold tracking-tight text-white flex items-center gap-2">
                  Talgo Tracker
                  <span className="text-[11px] font-semibold tracking-wide uppercase px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-300 border border-rose-500/30">
                    DK-DE Corridor
                  </span>
                </h1>
              </div>
              <p className="text-xs text-slate-400">
                Rolling stock stats & equipment history for Hamburg ➔ Copenhagen trains
              </p>
            </div>
          </div>

          {/* Navigation Tabs */}
          <div className="flex items-center bg-slate-950 p-1 rounded-xl border border-slate-800/80">
            <button
              onClick={() => setActiveTab('overview')}
              className={`flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-xs font-medium transition-all ${
                activeTab === 'overview'
                  ? 'bg-slate-800 text-white shadow-sm border border-slate-700'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <BarChart3 className="h-3.5 w-3.5" />
              Corridor Overview
            </button>

            <button
              onClick={() => setActiveTab('detail')}
              className={`flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-xs font-medium transition-all ${
                activeTab === 'detail'
                  ? 'bg-rose-600 text-white shadow-sm shadow-rose-900/30'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Layers className="h-3.5 w-3.5" />
              Stats per Train ID
              {selectedTrainId && (
                <span className="ml-1 px-1.5 py-0.2 rounded bg-slate-900/60 text-[10px] font-mono text-rose-200">
                  #{selectedTrainId}
                </span>
              )}
            </button>

            <button
              onClick={() => setActiveTab('compare')}
              className={`flex items-center gap-2 px-3.5 py-1.5 rounded-lg text-xs font-medium transition-all ${
                activeTab === 'compare'
                  ? 'bg-slate-800 text-white shadow-sm border border-slate-700'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <GitCompare className="h-3.5 w-3.5" />
              Compare
            </button>
          </div>

          {/* Quick Search & Total Logs Pill */}
          <div className="flex items-center gap-3">
            <div className="relative">
              <div className="relative flex items-center">
                <Search className="h-3.5 w-3.5 absolute left-3 text-slate-400 pointer-events-none" />
                <input
                  type="text"
                  placeholder="Find Train ID (e.g. 396)..."
                  value={searchInput}
                  onChange={(e) => {
                    setSearchInput(e.target.value);
                    setIsSearching(true);
                  }}
                  onFocus={() => setIsSearching(true)}
                  className="w-48 sm:w-56 bg-slate-950 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-rose-500 focus:ring-1 focus:ring-rose-500/50"
                />
              </div>

              {/* Quick Search Dropdown */}
              {isSearching && filteredTrainIds.length > 0 && (
                <div className="absolute right-0 mt-1.5 w-64 bg-slate-900 border border-slate-800 rounded-xl shadow-2xl z-50 py-1.5 max-h-60 overflow-y-auto">
                  <div className="px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400 border-b border-slate-800">
                    Jump to Train Stats
                  </div>
                  {filteredTrainIds.map((id) => (
                    <button
                      key={id}
                      onClick={() => handleSelectTrain(id)}
                      className="w-full text-left px-3 py-2 text-xs text-slate-200 hover:bg-slate-800 hover:text-rose-400 flex items-center justify-between"
                    >
                      <span className="font-mono font-bold">Train #{id}</span>
                      <span className="text-[10px] text-slate-400">View detailed stats ➔</span>
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div className="hidden lg:flex items-center gap-1.5 text-xs text-slate-400 bg-slate-950/80 px-3 py-1.5 rounded-xl border border-slate-800">
              <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse"></span>
              <span className="font-medium text-slate-300">{totalLogsCount.toLocaleString()}</span>
              <span>logs</span>
            </div>
          </div>

        </div>
      </div>
    </header>
  );
};
