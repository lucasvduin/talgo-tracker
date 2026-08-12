import React from 'react';
import { TrainLog, TrainStats } from '../types';
import { CATEGORY_COLORS, computeTrainStats } from '../lib/dataParser';
import { CarriageVisualizer } from './CarriageVisualizer';
import { GitCompare, Train, Clock, MapPin, AlertTriangle, CheckCircle2, Layers, ArrowRight } from 'lucide-react';

interface TrainComparisonViewProps {
  allTrainIds: string[];
  allLogs: TrainLog[];
  defaultTrainA?: string;
  defaultTrainB?: string;
}

export const TrainComparisonView: React.FC<TrainComparisonViewProps> = ({
  allTrainIds,
  allLogs,
  defaultTrainA = '396',
  defaultTrainB = '397',
}) => {
  const [trainAId, setTrainAId] = React.useState<string>(
    allTrainIds.includes(defaultTrainA) ? defaultTrainA : allTrainIds[0] || '396'
  );
  const [trainBId, setTrainBId] = React.useState<string>(
    allTrainIds.includes(defaultTrainB) ? defaultTrainB : allTrainIds[1] || '397'
  );

  const statsA: TrainStats = React.useMemo(() => computeTrainStats(trainAId, allLogs), [trainAId, allLogs]);
  const statsB: TrainStats = React.useMemo(() => computeTrainStats(trainBId, allLogs), [trainBId, allLogs]);

  const colorA = CATEGORY_COLORS[statsA.primaryType] || '#e63946';
  const colorB = CATEGORY_COLORS[statsB.primaryType] || '#005baa';

  return (
    <div className="space-y-8">
      
      {/* Header Controls for Selection */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl">
        <div className="flex flex-col md:flex-row items-center justify-between gap-6">
          
          {/* Train A Select */}
          <div className="w-full md:w-1/2 bg-slate-950 p-4 rounded-xl border border-slate-800">
            <label className="text-xs uppercase font-semibold text-slate-400 block mb-2">
              Select First Train Number
            </label>
            <div className="flex items-center gap-2">
              <span className="font-mono font-bold text-lg text-rose-400">#</span>
              <select
                value={trainAId}
                onChange={(e) => setTrainAId(e.target.value)}
                className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-2 text-white font-mono font-bold focus:outline-none focus:border-rose-500"
              >
                {allTrainIds.map((id) => (
                  <option key={id} value={id}>
                    Train #{id} ({computeTrainStats(id, allLogs).primaryType})
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="flex items-center justify-center bg-slate-800 p-2.5 rounded-full text-slate-400">
            <GitCompare className="h-5 w-5 text-rose-400" />
          </div>

          {/* Train B Select */}
          <div className="w-full md:w-1/2 bg-slate-950 p-4 rounded-xl border border-slate-800">
            <label className="text-xs uppercase font-semibold text-slate-400 block mb-2">
              Select Second Train Number
            </label>
            <div className="flex items-center gap-2">
              <span className="font-mono font-bold text-lg text-sky-400">#</span>
              <select
                value={trainBId}
                onChange={(e) => setTrainBId(e.target.value)}
                className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-2 text-white font-mono font-bold focus:outline-none focus:border-sky-500"
              >
                {allTrainIds.map((id) => (
                  <option key={id} value={id}>
                    Train #{id} ({computeTrainStats(id, allLogs).primaryType})
                  </option>
                ))}
              </select>
            </div>
          </div>

        </div>
      </div>

      {/* Side-by-Side Comparison Matrix */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        
        {/* Train A Card */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6 relative overflow-hidden">
          <div
            className="absolute top-0 right-0 w-48 h-48 rounded-full blur-3xl opacity-10 pointer-events-none"
            style={{ backgroundColor: colorA }}
          ></div>

          <div className="flex items-center justify-between border-b border-slate-800 pb-4">
            <div>
              <span className="text-2xl font-mono font-extrabold text-white">Train #{statsA.trainId}</span>
              <div className="text-xs text-slate-400 mt-0.5">{statsA.primaryRoute}</div>
            </div>
            <span
              className="px-3 py-1 rounded-full text-xs font-bold border"
              style={{
                backgroundColor: `${colorA}20`,
                borderColor: `${colorA}50`,
                color: colorA === '#1d3557' ? '#93c5fd' : colorA,
              }}
            >
              {statsA.primaryType}
            </span>
          </div>

          <div className="grid grid-cols-2 gap-3 text-xs">
            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Total Logs</span>
              <span className="text-lg font-mono font-bold text-white">{statsA.totalLogs}</span>
            </div>

            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Cancellation Rate</span>
              <span className={`text-lg font-mono font-bold ${statsA.cancellationRate > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                {statsA.cancellationRate}%
              </span>
            </div>

            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Typical Time</span>
              <span className="text-sm font-mono font-bold text-slate-200">{statsA.typicalScheduledTime}</span>
            </div>

            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Carriage Count</span>
              <span className="text-sm font-mono font-bold text-slate-200">
                {statsA.mostCommonCarCount > 0 ? `${statsA.mostCommonCarCount} cars` : 'Variable'}
              </span>
            </div>
          </div>

          {/* Equipment Mix Breakdown for A */}
          <div className="space-y-2">
            <span className="text-xs font-semibold text-slate-300 block">Equipment Breakdown:</span>
            <div className="space-y-1.5">
              {Object.entries(statsA.equipmentDistribution).map(([cat, count]) => {
                const pct = Math.round((count / statsA.totalLogs) * 100);
                const cColor = CATEGORY_COLORS[cat] || '#718096';
                return (
                  <div key={cat} className="space-y-1">
                    <div className="flex justify-between text-[11px] text-slate-400">
                      <span>{cat}</span>
                      <span className="font-mono text-slate-200">{count} ({pct}%)</span>
                    </div>
                    <div className="h-1.5 w-full bg-slate-950 rounded-full overflow-hidden">
                      <div className="h-full" style={{ width: `${pct}%`, backgroundColor: cColor }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Primary Formation for A */}
          {statsA.unitConfigurations.length > 0 && (
            <div className="space-y-2 pt-2 border-t border-slate-800">
              <span className="text-xs font-semibold text-slate-300 block">Top Unit Formation:</span>
              <CarriageVisualizer rawUnits={statsA.unitConfigurations[0].config} trainType={statsA.primaryType} />
            </div>
          )}
        </div>

        {/* Train B Card */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6 relative overflow-hidden">
          <div
            className="absolute top-0 right-0 w-48 h-48 rounded-full blur-3xl opacity-10 pointer-events-none"
            style={{ backgroundColor: colorB }}
          ></div>

          <div className="flex items-center justify-between border-b border-slate-800 pb-4">
            <div>
              <span className="text-2xl font-mono font-extrabold text-white">Train #{statsB.trainId}</span>
              <div className="text-xs text-slate-400 mt-0.5">{statsB.primaryRoute}</div>
            </div>
            <span
              className="px-3 py-1 rounded-full text-xs font-bold border"
              style={{
                backgroundColor: `${colorB}20`,
                borderColor: `${colorB}50`,
                color: colorB === '#1d3557' ? '#93c5fd' : colorB,
              }}
            >
              {statsB.primaryType}
            </span>
          </div>

          <div className="grid grid-cols-2 gap-3 text-xs">
            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Total Logs</span>
              <span className="text-lg font-mono font-bold text-white">{statsB.totalLogs}</span>
            </div>

            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Cancellation Rate</span>
              <span className={`text-lg font-mono font-bold ${statsB.cancellationRate > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                {statsB.cancellationRate}%
              </span>
            </div>

            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Typical Time</span>
              <span className="text-sm font-mono font-bold text-slate-200">{statsB.typicalScheduledTime}</span>
            </div>

            <div className="bg-slate-950 p-3 rounded-xl border border-slate-800">
              <span className="text-slate-400 block text-[10px] uppercase">Carriage Count</span>
              <span className="text-sm font-mono font-bold text-slate-200">
                {statsB.mostCommonCarCount > 0 ? `${statsB.mostCommonCarCount} cars` : 'Variable'}
              </span>
            </div>
          </div>

          {/* Equipment Mix Breakdown for B */}
          <div className="space-y-2">
            <span className="text-xs font-semibold text-slate-300 block">Equipment Breakdown:</span>
            <div className="space-y-1.5">
              {Object.entries(statsB.equipmentDistribution).map(([cat, count]) => {
                const pct = Math.round((count / statsB.totalLogs) * 100);
                const cColor = CATEGORY_COLORS[cat] || '#718096';
                return (
                  <div key={cat} className="space-y-1">
                    <div className="flex justify-between text-[11px] text-slate-400">
                      <span>{cat}</span>
                      <span className="font-mono text-slate-200">{count} ({pct}%)</span>
                    </div>
                    <div className="h-1.5 w-full bg-slate-950 rounded-full overflow-hidden">
                      <div className="h-full" style={{ width: `${pct}%`, backgroundColor: cColor }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Primary Formation for B */}
          {statsB.unitConfigurations.length > 0 && (
            <div className="space-y-2 pt-2 border-t border-slate-800">
              <span className="text-xs font-semibold text-slate-300 block">Top Unit Formation:</span>
              <CarriageVisualizer rawUnits={statsB.unitConfigurations[0].config} trainType={statsB.primaryType} />
            </div>
          )}
        </div>

      </div>
    </div>
  );
};
