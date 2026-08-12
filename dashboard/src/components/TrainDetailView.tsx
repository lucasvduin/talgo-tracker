import React from 'react';
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from 'recharts';
import {
  Train,
  Clock,
  MapPin,
  AlertTriangle,
  CheckCircle2,
  Calendar,
  Layers,
  Sparkles,
  ChevronLeft,
  ChevronRight,
  TrendingUp,
  Info,
  ShieldCheck,
} from 'lucide-react';
import { TrainLog, TrainStats } from '../types';
import { CATEGORY_COLORS, computeTrainStats, getStationName } from '../lib/dataParser';
import { CarriageVisualizer } from './CarriageVisualizer';
import { LogTable } from './LogTable';

interface TrainDetailViewProps {
  selectedTrainId: string;
  onSelectTrain: (trainId: string) => void;
  allTrainIds: string[];
  allLogs: TrainLog[];
}

export const TrainDetailView: React.FC<TrainDetailViewProps> = ({
  selectedTrainId,
  onSelectTrain,
  allTrainIds,
  allLogs,
}) => {
  const trainStats: TrainStats = React.useMemo(() => {
    return computeTrainStats(selectedTrainId, allLogs);
  }, [selectedTrainId, allLogs]);

  const trainLogs = React.useMemo(() => {
    return allLogs.filter((l) => l.trainId === selectedTrainId);
  }, [selectedTrainId, allLogs]);

  // Index in allTrainIds for prev/next
  const currentIndex = allTrainIds.indexOf(selectedTrainId);
  const prevTrainId = currentIndex > 0 ? allTrainIds[currentIndex - 1] : null;
  const nextTrainId = currentIndex < allTrainIds.length - 1 ? allTrainIds[currentIndex + 1] : null;

  // Pie chart data for this train's equipment distribution
  const equipmentPieData = React.useMemo(() => {
    return Object.entries(trainStats.equipmentDistribution).map(([cat, count]) => ({
      name: cat,
      value: count,
      color: CATEGORY_COLORS[cat] || '#718096',
    }));
  }, [trainStats]);

  // Primary color for this train
  const mainColor = CATEGORY_COLORS[trainStats.primaryType] || '#e63946';

  return (
    <div className="space-y-8">
      
      {/* Train Selector Header Bar */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-5 shadow-xl flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <button
              disabled={!prevTrainId}
              onClick={() => prevTrainId && onSelectTrain(prevTrainId)}
              className="p-2 rounded-xl bg-slate-950 border border-slate-800 text-slate-300 disabled:opacity-30 disabled:cursor-not-allowed hover:bg-slate-800 transition-colors"
              title="Previous Train ID"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>

            <div className="flex items-center gap-2 bg-slate-950 px-3.5 py-1.5 rounded-xl border border-slate-800">
              <span className="text-xs text-slate-400 font-medium">Train Number:</span>
              <select
                value={selectedTrainId}
                onChange={(e) => onSelectTrain(e.target.value)}
                className="bg-transparent font-mono font-bold text-base text-rose-400 focus:outline-none cursor-pointer"
              >
                {allTrainIds.map((id) => (
                  <option key={id} value={id} className="bg-slate-900 text-white font-mono">
                    Train #{id}
                  </option>
                ))}
              </select>
            </div>

            <button
              disabled={!nextTrainId}
              onClick={() => nextTrainId && onSelectTrain(nextTrainId)}
              className="p-2 rounded-xl bg-slate-950 border border-slate-800 text-slate-300 disabled:opacity-30 disabled:cursor-not-allowed hover:bg-slate-800 transition-colors"
              title="Next Train ID"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Quick Pills for Major Train Numbers */}
        <div className="flex items-center gap-1.5 overflow-x-auto pb-1 sm:pb-0 scrollbar-none">
          <span className="text-[11px] text-slate-400 mr-1 flex-shrink-0">Quick jump:</span>
          {['396', '397', '394', '398', '399', '1172', '1175', '1199'].map((id) => (
            <button
              key={id}
              onClick={() => onSelectTrain(id)}
              className={`px-2.5 py-1 rounded-lg text-xs font-mono font-semibold transition-all flex-shrink-0 ${
                selectedTrainId === id
                  ? 'bg-rose-600 text-white shadow-md shadow-rose-900/40'
                  : 'bg-slate-950 text-slate-400 hover:text-white border border-slate-800'
              }`}
            >
              #{id}
            </button>
          ))}
        </div>
      </div>

      {/* Selected Train Hero Banner */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
        <div
          className="absolute top-0 right-0 w-72 h-72 rounded-full blur-3xl opacity-15 pointer-events-none"
          style={{ backgroundColor: mainColor }}
        ></div>

        <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-6 relative z-10">
          
          {/* Main Title & Tags */}
          <div className="space-y-3">
            <div className="flex flex-wrap items-center gap-3">
              <span
                className="text-2xl font-mono font-extrabold px-3.5 py-1 rounded-xl text-white shadow-lg border"
                style={{
                  backgroundColor: `${mainColor}25`,
                  borderColor: `${mainColor}60`,
                }}
              >
                Train #{selectedTrainId}
              </span>

              <span
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-bold border"
                style={{
                  backgroundColor: `${mainColor}20`,
                  borderColor: `${mainColor}50`,
                  color: mainColor === '#1d3557' ? '#93c5fd' : mainColor,
                }}
              >
                <Train className="h-3.5 w-3.5" />
                Primary: {trainStats.primaryType} ({trainStats.primaryTypePercentage}%)
              </span>

              {trainStats.cancellationRate > 0 ? (
                <span className="inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-semibold bg-amber-500/10 text-amber-400 border border-amber-500/30">
                  <AlertTriangle className="h-3.5 w-3.5" />
                  {trainStats.cancellationRate}% Cancelled
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  100% Scheduled Reliability
                </span>
              )}
            </div>

            <div className="text-xl font-bold text-white flex items-center gap-2">
              <MapPin className="h-5 w-5 text-rose-500 flex-shrink-0" />
              <span>{trainStats.primaryRoute}</span>
            </div>

            <p className="text-xs text-slate-400">
              Tracked across <span className="text-slate-200 font-semibold">{trainStats.totalLogs} logs</span> between{' '}
              <span className="font-mono text-slate-300">{trainStats.firstSeenDate}</span> and{' '}
              <span className="font-mono text-slate-300">{trainStats.lastSeenDate}</span>
            </p>
          </div>

          {/* Quick Metrics Badges Grid */}
          <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
            <div className="bg-slate-950/80 border border-slate-800/80 rounded-xl p-3">
              <span className="text-[10px] uppercase font-semibold text-slate-400 block mb-0.5">
                Typical Padborg Time
              </span>
              <div className="text-base font-mono font-bold text-white flex items-center gap-1.5">
                <Clock className="h-4 w-4 text-rose-400" />
                {trainStats.typicalScheduledTime}
              </div>
            </div>

            <div className="bg-slate-950/80 border border-slate-800/80 rounded-xl p-3">
              <span className="text-[10px] uppercase font-semibold text-slate-400 block mb-0.5">
                Average Cars
              </span>
              <div className="text-base font-mono font-bold text-white">
                {trainStats.avgCarCount > 0 ? `${trainStats.avgCarCount} cars` : 'N/A'}
              </div>
            </div>

            <div className="bg-slate-950/80 border border-slate-800/80 rounded-xl p-3 col-span-2 sm:col-span-1">
              <span className="text-[10px] uppercase font-semibold text-slate-400 block mb-0.5">
                Primary Formation
              </span>
              <div className="text-base font-mono font-bold text-rose-300">
                {trainStats.mostCommonCarCount > 0 ? `${trainStats.mostCommonCarCount} carriages` : 'Variable'}
              </div>
            </div>
          </div>

        </div>
      </div>

      {/* Equipment Mix & Carriage Distribution Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Chart 1: Equipment Distribution Pie */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg">
          <h3 className="text-base font-bold text-white mb-1 flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-rose-400" />
            Equipment Type Distribution for Train #{selectedTrainId}
          </h3>
          <p className="text-xs text-slate-400 mb-4">
            Rolling stock categories logged for this specific train number over time
          </p>

          <div className="h-56 w-full flex items-center justify-center">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={equipmentPieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={50}
                  outerRadius={80}
                  paddingAngle={4}
                  dataKey="value"
                >
                  {equipmentPieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#0f172a',
                    borderColor: '#334155',
                    borderRadius: '8px',
                    color: '#fff',
                    fontSize: '12px',
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>

          <div className="grid grid-cols-2 gap-2 mt-2 pt-2 border-t border-slate-800">
            {equipmentPieData.map((item) => (
              <div key={item.name} className="flex items-center justify-between text-xs p-1.5 rounded-lg bg-slate-950">
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: item.color }}></span>
                  <span className="text-slate-300 font-medium">{item.name}</span>
                </div>
                <span className="font-mono text-white font-bold">{item.value}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Chart 2: Carriage Count Distribution */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg">
          <h3 className="text-base font-bold text-white mb-1 flex items-center gap-2">
            <Layers className="h-4 w-4 text-sky-400" />
            Carriage Count Frequency
          </h3>
          <p className="text-xs text-slate-400 mb-4">
            Distribution of total cars per trainset recorded for Train #{selectedTrainId}
          </p>

          <div className="h-56 w-full">
            {trainStats.carCountDistribution.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={trainStats.carCountDistribution}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                  <XAxis dataKey="cars" stroke="#94a3b8" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#94a3b8" tick={{ fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#0f172a',
                      borderColor: '#334155',
                      borderRadius: '8px',
                      color: '#fff',
                      fontSize: '12px',
                    }}
                  />
                  <Bar dataKey="count" fill="#38bdf8" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-full flex items-center justify-center text-xs text-slate-500 italic">
                No carriage count data available
              </div>
            )}
          </div>

          <div className="mt-3 text-xs text-slate-400 bg-slate-950 p-2.5 rounded-xl border border-slate-800 flex items-center justify-between">
            <span>Primary Coach Count:</span>
            <span className="font-mono font-bold text-sky-400">
              {trainStats.mostCommonCarCount > 0 ? `${trainStats.mostCommonCarCount} Cars` : 'Variable'}
            </span>
          </div>
        </div>

      </div>

      {/* Frequent Formations & Unit Sequences */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
        <div>
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            <Train className="h-5 w-5 text-amber-400" />
            Logged Formations & Unit Sequences
          </h3>
          <p className="text-xs text-slate-400 mt-0.5">
            Most frequent locomotive and carriage configurations recorded for Train #{selectedTrainId}
          </p>
        </div>

        <div className="space-y-3">
          {trainStats.unitConfigurations.length === 0 ? (
            <div className="text-xs text-slate-500 italic py-4">
              No detailed unit configurations logged for this train number.
            </div>
          ) : (
            trainStats.unitConfigurations.map((item, idx) => (
              <div
                key={idx}
                className="bg-slate-950 border border-slate-800 rounded-xl p-4 space-y-2 hover:border-slate-700 transition-all"
              >
                <div className="flex items-center justify-between text-xs">
                  <span className="font-semibold text-amber-300 flex items-center gap-1.5">
                    <span className="h-2 w-2 rounded-full bg-amber-400"></span>
                    Formation Option #{idx + 1}
                  </span>
                  <span className="font-mono text-slate-400 bg-slate-900 px-2 py-0.5 rounded border border-slate-800">
                    Logged {item.count} times ({Math.round((item.count / trainStats.totalLogs) * 100)}%)
                  </span>
                </div>

                <CarriageVisualizer
                  rawUnits={item.config}
                  trainType={trainStats.primaryType}
                />
              </div>
            ))
          )}
        </div>
      </div>

      {/* Chronological Log Table Scoped to Selected Train */}
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            <Calendar className="h-4 w-4 text-rose-400" />
            Complete Historical Logs for Train #{selectedTrainId}
          </h3>
          <span className="text-xs text-slate-400">Showing all {trainLogs.length} entries</span>
        </div>

        <LogTable logs={trainLogs} showTrainIdColumn={false} />
      </div>

    </div>
  );
};
