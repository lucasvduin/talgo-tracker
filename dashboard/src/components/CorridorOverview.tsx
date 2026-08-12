import React from 'react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import {
  Train,
  Activity,
  AlertTriangle,
  Sparkles,
  ArrowRight,
  TrendingUp,
  Layers,
  ShieldAlert,
  CheckCircle2,
} from 'lucide-react';
import { CorridorStats, TrainLog, TrainStats } from '../types';
import { CATEGORY_COLORS, computeTrainStats, getStationName } from '../lib/dataParser';

interface CorridorOverviewProps {
  corridorStats: CorridorStats;
  allLogs: TrainLog[];
  allTrainIds: string[];
  onSelectTrain: (trainId: string) => void;
}

export const CorridorOverview: React.FC<CorridorOverviewProps> = ({
  corridorStats,
  allLogs,
  allTrainIds,
  onSelectTrain,
}) => {
  // Filter out minor train IDs with < 3 logs for clean chart, but keep top 25
  const trainStatsList: TrainStats[] = React.useMemo(() => {
    return allTrainIds
      .map((id) => computeTrainStats(id, allLogs))
      .sort((a, b) => b.totalLogs - a.totalLogs);
  }, [allTrainIds, allLogs]);

  // Chart data for top 20 trains
  const topChartData = React.useMemo(() => {
    return trainStatsList
      .slice(0, 22)
      .sort((a, b) => parseInt(a.trainId) - parseInt(b.trainId))
      .map((st) => {
        return {
          trainId: `#${st.trainId}`,
          rawId: st.trainId,
          Talgo: st.equipmentDistribution['Talgo'] || 0,
          Railjet: st.equipmentDistribution['Railjet'] || 0,
          IC3: st.equipmentDistribution['IC3'] || 0,
          'German IC Coaches': st.equipmentDistribution['German IC Coaches'] || 0,
          'Vectron-hauled': st.equipmentDistribution['Vectron-hauled'] || 0,
          Unknown: st.equipmentDistribution['Unknown'] || 0,
          Cancelled: st.equipmentDistribution['Cancelled'] || 0,
          total: st.totalLogs,
        };
      });
  }, [trainStatsList]);

  // Overall equipment breakdown pie chart
  const pieData = React.useMemo(() => {
    return Object.entries(corridorStats.equipmentTotals).map(([category, count]) => ({
      name: category,
      value: count,
      color: CATEGORY_COLORS[category] || '#a0aec0',
    }));
  }, [corridorStats]);

  return (
    <div className="space-y-8">
      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        
        {/* Card 1: Total Logged Trains */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg relative overflow-hidden group hover:border-slate-700 transition-all">
          <div className="absolute top-0 right-0 w-24 h-24 bg-rose-500/5 rounded-full blur-2xl group-hover:bg-rose-500/10 transition-all"></div>
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-medium uppercase tracking-wider">Total Recorded Logs</span>
            <div className="p-2 rounded-xl bg-rose-500/10 text-rose-400">
              <Activity className="h-4 w-4" />
            </div>
          </div>
          <div className="text-3xl font-bold text-white tracking-tight">
            {corridorStats.totalLogs.toLocaleString()}
          </div>
          <div className="mt-2 text-xs text-slate-400 flex items-center gap-1.5">
            <span className="text-emerald-400 font-semibold">{corridorStats.uniqueTrainIds}</span>
            <span>unique train numbers tracked</span>
          </div>
        </div>

        {/* Card 2: Talgo Fleet Deployment */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg relative overflow-hidden group hover:border-slate-700 transition-all">
          <div className="absolute top-0 right-0 w-24 h-24 bg-red-500/5 rounded-full blur-2xl group-hover:bg-red-500/10 transition-all"></div>
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-medium uppercase tracking-wider">Talgo 230 Deployment</span>
            <div className="p-2 rounded-xl bg-red-500/10 text-red-400">
              <Train className="h-4 w-4" />
            </div>
          </div>
          <div className="text-3xl font-bold text-white tracking-tight flex items-baseline gap-2">
            {(corridorStats.equipmentTotals['Talgo'] || 0).toLocaleString()}
            <span className="text-xs font-semibold text-rose-400">
              ({Math.round(((corridorStats.equipmentTotals['Talgo'] || 0) / corridorStats.totalLogs) * 100)}%)
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-400">
            Total logged Talgo 230 trainset runs
          </div>
        </div>

        {/* Card 3: Overall Cancellation Rate */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg relative overflow-hidden group hover:border-slate-700 transition-all">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-medium uppercase tracking-wider">Cancellation Rate</span>
            <div className="p-2 rounded-xl bg-amber-500/10 text-amber-400">
              <AlertTriangle className="h-4 w-4" />
            </div>
          </div>
          <div className="text-3xl font-bold text-white tracking-tight flex items-baseline gap-2">
            {corridorStats.overallCancellationRate}%
            <span className="text-xs font-normal text-slate-400">
              ({corridorStats.totalCancelled} cancelled)
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-400">
            Across all scheduled corridor services
          </div>
        </div>

        {/* Card 4: Top Active Services */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg relative overflow-hidden group hover:border-slate-700 transition-all">
          <div className="flex items-center justify-between text-slate-400 mb-3">
            <span className="text-xs font-medium uppercase tracking-wider">Main Corridor Routes</span>
            <div className="p-2 rounded-xl bg-sky-500/10 text-sky-400">
              <Layers className="h-4 w-4" />
            </div>
          </div>
          <div className="text-sm font-semibold text-slate-200">
            København H ➔ Hamburg Hbf
          </div>
          <div className="mt-2 text-xs text-slate-400 flex items-center justify-between">
            <span>EC & IC International Services</span>
            <span className="text-sky-400 font-mono text-[11px]">DSB / DB / ČD</span>
          </div>
        </div>

      </div>

      {/* Main Stacked Bar Chart: Equipment Breakdown by Train ID */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
          <div>
            <h2 className="text-lg font-bold text-white flex items-center gap-2">
              <TrendingUp className="h-5 w-5 text-rose-500" />
              Equipment Classification History by Train ID
            </h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Cumulative rolling stock composition across the top tracked train numbers (Click any bar to jump to stats)
            </p>
          </div>

          <div className="flex items-center gap-2 text-xs text-slate-400 bg-slate-950 px-3 py-1.5 rounded-xl border border-slate-800">
            <span>Showing top 22 high-volume train IDs</span>
          </div>
        </div>

        <div className="h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={topChartData}
              onClick={(state: any) => {
                if (state && state.activePayload && state.activePayload.length > 0) {
                  const rawId = state.activePayload[0].payload.rawId;
                  if (rawId) onSelectTrain(rawId);
                }
              }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="trainId" stroke="#94a3b8" tick={{ fontSize: 11 }} />
              <YAxis stroke="#94a3b8" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#0f172a',
                  borderColor: '#334155',
                  borderRadius: '12px',
                  color: '#fff',
                  fontSize: '12px',
                  boxShadow: '0 20px 25px -5px rgb(0 0 0 / 0.5)',
                }}
                cursor={{ fill: 'rgba(255, 255, 255, 0.05)' }}
              />
              <Legend
                wrapperStyle={{ fontSize: '11px', paddingTop: '10px' }}
                iconType="circle"
              />
              <Bar dataKey="Talgo" stackId="a" fill={CATEGORY_COLORS['Talgo']} cursor="pointer" />
              <Bar dataKey="Railjet" stackId="a" fill={CATEGORY_COLORS['Railjet']} cursor="pointer" />
              <Bar dataKey="IC3" stackId="a" fill={CATEGORY_COLORS['IC3']} cursor="pointer" />
              <Bar
                dataKey="German IC Coaches"
                stackId="a"
                fill={CATEGORY_COLORS['German IC Coaches']}
                cursor="pointer"
              />
              <Bar
                dataKey="Vectron-hauled"
                stackId="a"
                fill={CATEGORY_COLORS['Vectron-hauled']}
                cursor="pointer"
              />
              <Bar
                dataKey="Cancelled"
                stackId="a"
                fill={CATEGORY_COLORS['Cancelled']}
                cursor="pointer"
              />
              <Bar
                dataKey="Unknown"
                stackId="a"
                fill={CATEGORY_COLORS['Unknown']}
                cursor="pointer"
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Grid of Train Numbers (Quick Access Cards) */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            <Layers className="h-4 w-4 text-rose-400" />
            Select a Train ID for Detailed Stats
          </h3>
          <span className="text-xs text-slate-400">Click any train card for full analytics</span>
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
          {trainStatsList.slice(0, 24).map((st) => {
            const talgoCount = st.equipmentDistribution['Talgo'] || 0;
            const talgoPct = st.scheduledCount > 0 ? Math.round((talgoCount / st.scheduledCount) * 100) : 0;
            const isTalgoDominated = talgoPct >= 50;

            return (
              <button
                key={st.trainId}
                onClick={() => onSelectTrain(st.trainId)}
                className="bg-slate-900 border border-slate-800 rounded-xl p-3.5 text-left hover:border-rose-500/60 hover:bg-slate-800/80 transition-all group flex flex-col justify-between shadow-sm relative overflow-hidden"
              >
                <div>
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-base font-mono font-bold text-white group-hover:text-rose-400 transition-colors">
                      #{st.trainId}
                    </span>
                    <span className="text-[10px] text-slate-400 bg-slate-950 px-1.5 py-0.5 rounded border border-slate-800">
                      {st.totalLogs} logs
                    </span>
                  </div>

                  <div className="text-[11px] text-slate-300 font-medium truncate mb-2">
                    {st.primaryRoute}
                  </div>
                </div>

                <div>
                  {/* Equipment Mini Progress Bar */}
                  <div className="h-1.5 w-full bg-slate-950 rounded-full overflow-hidden flex mb-2">
                    {Object.entries(st.equipmentDistribution).map(([cat, count]) => {
                      const pct = (count / st.totalLogs) * 100;
                      return (
                        <div
                          key={cat}
                          style={{
                            width: `${pct}%`,
                            backgroundColor: CATEGORY_COLORS[cat] || '#718096',
                          }}
                          className="h-full"
                          title={`${cat}: ${count} (${Math.round(pct)}%)`}
                        />
                      );
                    })}
                  </div>

                  <div className="flex items-center justify-between text-[10px]">
                    <span className="text-slate-400 font-medium truncate max-w-[80px]">
                      {st.primaryType}
                    </span>
                    <span
                      className={`font-semibold ${
                        st.cancellationRate > 15
                          ? 'text-amber-400'
                          : st.cancellationRate > 0
                          ? 'text-slate-400'
                          : 'text-emerald-400'
                      }`}
                    >
                      {st.cancellationRate}% cnl
                    </span>
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Fleet Breakdown & Highlights Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* Overall Rolling Stock Distribution */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg flex flex-col justify-between">
          <div>
            <h3 className="text-sm font-bold text-white mb-1 flex items-center gap-2">
              <Sparkles className="h-4 w-4 text-amber-400" />
              Overall Fleet Equipment Distribution
            </h3>
            <p className="text-xs text-slate-400 mb-4">
              Breakdown across all 1,900+ logged train arrivals & departures
            </p>

            <div className="h-48 w-full flex items-center justify-center">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={45}
                    outerRadius={70}
                    paddingAngle={3}
                    dataKey="value"
                  >
                    {pieData.map((entry, index) => (
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
          </div>

          <div className="space-y-1.5 mt-2">
            {pieData.map((item) => (
              <div key={item.name} className="flex items-center justify-between text-xs">
                <div className="flex items-center gap-2">
                  <span
                    className="h-2.5 w-2.5 rounded-full"
                    style={{ backgroundColor: item.color }}
                  ></span>
                  <span className="text-slate-300 font-medium">{item.name}</span>
                </div>
                <div className="font-mono text-slate-400">
                  {item.value}{' '}
                  <span className="text-[10px] text-slate-400">
                    ({Math.round((item.value / corridorStats.totalLogs) * 100)}%)
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* High Talgo Usage Services */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg">
          <h3 className="text-sm font-bold text-white mb-1 flex items-center gap-2">
            <Train className="h-4 w-4 text-red-500" />
            Top Talgo 230 Dedicated Services
          </h3>
          <p className="text-xs text-slate-400 mb-4">
            Train numbers with the highest logged Talgo trainset deployments
          </p>

          <div className="space-y-3">
            {corridorStats.topTalgoTrainIds.map((id) => {
              const st = computeTrainStats(id, allLogs);
              const talgoCount = st.equipmentDistribution['Talgo'] || 0;
              const talgoPct = st.scheduledCount > 0 ? Math.round((talgoCount / st.scheduledCount) * 100) : 0;

              return (
                <div
                  key={id}
                  onClick={() => onSelectTrain(id)}
                  className="bg-slate-950 border border-slate-800 rounded-xl p-3 flex items-center justify-between hover:border-red-500/50 cursor-pointer transition-all group"
                >
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-mono font-bold text-white group-hover:text-red-400 transition-colors">
                        Train #{id}
                      </span>
                      <span className="text-[10px] px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 font-semibold border border-rose-500/30">
                        {talgoPct}% Talgo
                      </span>
                    </div>
                    <div className="text-xs text-slate-400 mt-1">
                      Route: <span className="text-slate-300">{st.primaryRoute}</span>
                    </div>
                  </div>

                  <div className="text-right">
                    <div className="text-sm font-mono font-bold text-white">{talgoCount}</div>
                    <div className="text-[10px] text-slate-400">Talgo runs</div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Corridor Reliability Highlights */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg">
          <h3 className="text-sm font-bold text-white mb-1 flex items-center gap-2">
            <ShieldAlert className="h-4 w-4 text-amber-400" />
            Reliability & Cancellation Watch
          </h3>
          <p className="text-xs text-slate-400 mb-4">
            Train numbers with highest recorded cancellation percentages
          </p>

          <div className="space-y-3">
            {corridorStats.highestCancellationTrainIds.map((item) => {
              const st = computeTrainStats(item.trainId, allLogs);
              return (
                <div
                  key={item.trainId}
                  onClick={() => onSelectTrain(item.trainId)}
                  className="bg-slate-950 border border-slate-800 rounded-xl p-3 flex items-center justify-between hover:border-amber-500/50 cursor-pointer transition-all group"
                >
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-mono font-bold text-white group-hover:text-amber-400 transition-colors">
                        Train #{item.trainId}
                      </span>
                      <span className="text-[10px] px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-semibold border border-amber-500/30">
                        {item.rate}% Cancelled
                      </span>
                    </div>
                    <div className="text-xs text-slate-400 mt-1">
                      Primary: <span className="text-slate-300">{st.primaryType}</span>
                    </div>
                  </div>

                  <div className="text-right">
                    <div className="text-sm font-mono font-bold text-amber-400">
                      {st.cancelledCount} / {st.totalLogs}
                    </div>
                    <div className="text-[10px] text-slate-400">Cancelled logs</div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

      </div>
    </div>
  );
};
