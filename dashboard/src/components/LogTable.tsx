import React from 'react';
import { TrainLog } from '../types';
import { CATEGORY_COLORS, getStationName } from '../lib/dataParser';
import { CarriageVisualizer } from './CarriageVisualizer';
import { Search, Filter, AlertTriangle, CheckCircle2, ChevronLeft, ChevronRight, Info, Eye } from 'lucide-react';

interface LogTableProps {
  logs: TrainLog[];
  showTrainIdColumn?: boolean;
}

export const LogTable: React.FC<LogTableProps> = ({ logs, showTrainIdColumn = true }) => {
  const [searchTerm, setSearchTerm] = React.useState('');
  const [statusFilter, setStatusFilter] = React.useState<'all' | 'scheduled' | 'cancelled'>('all');
  const [typeFilter, setTypeFilter] = React.useState<string>('all');
  const [currentPage, setCurrentPage] = React.useState(1);
  const [selectedLog, setSelectedLog] = React.useState<TrainLog | null>(null);

  const pageSize = 15;

  // Unique train types in these logs for filter dropdown
  const uniqueTypes = React.useMemo(() => {
    const types = new Set<string>();
    logs.forEach((l) => {
      if (l.trainType) types.add(l.trainType);
    });
    return Array.from(types);
  }, [logs]);

  // Filtered logs
  const filteredLogs = React.useMemo(() => {
    return logs.filter((log) => {
      // Status check
      if (statusFilter === 'scheduled' && log.isCancelled) return false;
      if (statusFilter === 'cancelled' && !log.isCancelled) return false;

      // Type check
      if (typeFilter !== 'all' && log.trainType !== typeFilter) return false;

      // Search term
      if (!searchTerm.trim()) return true;
      const term = searchTerm.toLowerCase();
      return (
        log.trainId.toLowerCase().includes(term) ||
        log.scheduledDate.toLowerCase().includes(term) ||
        log.origin.toLowerCase().includes(term) ||
        log.destination.toLowerCase().includes(term) ||
        log.trainType.toLowerCase().includes(term) ||
        log.rawUnits.toLowerCase().includes(term) ||
        log.status.toLowerCase().includes(term)
      );
    });
  }, [logs, statusFilter, typeFilter, searchTerm]);

  // Reset page on filter change
  React.useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, statusFilter, typeFilter]);

  const totalPages = Math.ceil(filteredLogs.length / pageSize) || 1;
  const paginatedLogs = React.useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return filteredLogs.slice(start, start + pageSize);
  }, [filteredLogs, currentPage, pageSize]);

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl shadow-xl overflow-hidden">
      
      {/* Table Header Controls */}
      <div className="p-4 sm:p-5 border-b border-slate-800 flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-4 bg-slate-950/60">
        <div className="flex items-center gap-2">
          <div className="relative flex-1 sm:w-64">
            <Search className="h-3.5 w-3.5 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              placeholder="Search logs (date, station, unit)..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full bg-slate-900 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-rose-500"
            />
          </div>

          {/* Status Filter */}
          <div className="flex items-center bg-slate-900 border border-slate-800 rounded-xl p-1 text-xs">
            <button
              onClick={() => setStatusFilter('all')}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium transition-all ${
                statusFilter === 'all'
                  ? 'bg-slate-800 text-white'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              All ({logs.length})
            </button>
            <button
              onClick={() => setStatusFilter('scheduled')}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium transition-all ${
                statusFilter === 'scheduled'
                  ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Scheduled
            </button>
            <button
              onClick={() => setStatusFilter('cancelled')}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium transition-all ${
                statusFilter === 'cancelled'
                  ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              Cancelled
            </button>
          </div>
        </div>

        {/* Type Dropdown */}
        {uniqueTypes.length > 1 && (
          <div className="flex items-center gap-2 text-xs">
            <span className="text-slate-400 text-[11px] hidden md:inline">Equipment:</span>
            <select
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
              className="bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-rose-500"
            >
              <option value="all">All Equipment Types</option>
              {uniqueTypes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
        )}
      </div>

      {/* Table Content */}
      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs text-slate-300 border-collapse">
          <thead>
            <tr className="bg-slate-950/80 text-slate-400 border-b border-slate-800 uppercase text-[10px] font-semibold tracking-wider">
              <th className="py-3 px-4">Date & Time</th>
              {showTrainIdColumn && <th className="py-3 px-4">Train ID</th>}
              <th className="py-3 px-4">Route</th>
              <th className="py-3 px-4">Equipment Type</th>
              <th className="py-3 px-4">Carriages / Formation</th>
              <th className="py-3 px-4">Status</th>
              <th className="py-3 px-4 text-right">Details</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60">
            {paginatedLogs.length === 0 ? (
              <tr>
                <td colSpan={showTrainIdColumn ? 7 : 6} className="py-8 text-center text-slate-500 italic">
                  No train logs match your search filters.
                </td>
              </tr>
            ) : (
              paginatedLogs.map((log) => {
                const color = log.isCancelled
                  ? CATEGORY_COLORS['Cancelled']
                  : CATEGORY_COLORS[log.trainType] || '#a0aec0';

                return (
                  <tr
                    key={log.id}
                    className={`hover:bg-slate-800/50 transition-colors ${
                      log.isCancelled ? 'bg-slate-950/40 text-slate-400' : ''
                    }`}
                  >
                    {/* Date & Time */}
                    <td className="py-3 px-4 font-mono">
                      <div className="text-white font-medium">{log.scheduledDate}</div>
                      <div className="text-[10px] text-slate-400">{log.scheduledTime}</div>
                    </td>

                    {/* Train ID */}
                    {showTrainIdColumn && (
                      <td className="py-3 px-4 font-mono font-bold text-rose-400">
                        #{log.trainId}
                      </td>
                    )}

                    {/* Route */}
                    <td className="py-3 px-4">
                      <div className="flex items-center gap-1.5 font-semibold text-slate-200">
                        <span title={getStationName(log.origin)} className="cursor-help underline decoration-dashed decoration-slate-600">
                          {log.origin}
                        </span>
                        <span className="text-rose-500 font-normal">➔</span>
                        <span title={getStationName(log.destination)} className="cursor-help underline decoration-dashed decoration-slate-600">
                          {log.destination}
                        </span>
                      </div>
                      <div className="text-[10px] text-slate-400 truncate max-w-[180px]">
                        {getStationName(log.origin).split(' ')[0]} to {getStationName(log.destination).split(' ')[0]}
                      </div>
                    </td>

                    {/* Equipment Type */}
                    <td className="py-3 px-4">
                      <span
                        className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-semibold border"
                        style={{
                          backgroundColor: `${color}15`,
                          borderColor: `${color}40`,
                          color: log.isCancelled ? '#a0aec0' : color,
                        }}
                      >
                        <span
                          className="h-1.5 w-1.5 rounded-full"
                          style={{ backgroundColor: color }}
                        ></span>
                        {log.trainType}
                      </span>
                    </td>

                    {/* Carriage Info */}
                    <td className="py-3 px-4 max-w-xs">
                      <div className="text-slate-300 font-medium truncate">
                        {log.carriageInfo || 'Standard Formation'}
                      </div>
                      {log.rawUnits && (
                        <div className="text-[10px] text-slate-400 font-mono truncate max-w-[200px]">
                          {log.rawUnits}
                        </div>
                      )}
                    </td>

                    {/* Status */}
                    <td className="py-3 px-4">
                      {log.isCancelled ? (
                        <span className="inline-flex items-center gap-1 text-rose-400 font-semibold text-[11px] bg-rose-500/10 px-2 py-0.5 rounded border border-rose-500/20">
                          <AlertTriangle className="h-3 w-3" />
                          {log.status}
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 text-emerald-400 font-medium text-[11px] bg-emerald-500/10 px-2 py-0.5 rounded border border-emerald-500/20">
                          <CheckCircle2 className="h-3 w-3" />
                          Scheduled
                        </span>
                      )}
                    </td>

                    {/* Details button */}
                    <td className="py-3 px-4 text-right">
                      <button
                        onClick={() => setSelectedLog(log)}
                        className="text-slate-400 hover:text-rose-400 bg-slate-950 p-1.5 rounded-lg border border-slate-800 transition-colors"
                        title="View carriage formation details"
                      >
                        <Eye className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination Footer */}
      <div className="p-4 border-t border-slate-800 flex items-center justify-between text-xs text-slate-400 bg-slate-950/60">
        <div>
          Showing <span className="text-slate-200 font-medium">{paginatedLogs.length}</span> of{' '}
          <span className="text-slate-200 font-medium">{filteredLogs.length}</span> logs
        </div>

        <div className="flex items-center gap-2">
          <button
            disabled={currentPage === 1}
            onClick={() => setCurrentPage((p) => Math.max(p - 1, 1))}
            className="p-1.5 rounded-lg bg-slate-900 border border-slate-800 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-800 text-slate-300"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>
          <span className="font-mono text-slate-300">
            Page {currentPage} of {totalPages}
          </span>
          <button
            disabled={currentPage === totalPages}
            onClick={() => setCurrentPage((p) => Math.min(p + 1, totalPages))}
            className="p-1.5 rounded-lg bg-slate-900 border border-slate-800 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-800 text-slate-300"
          >
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Carriage Details Modal */}
      {selectedLog && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-lg w-full p-6 shadow-2xl space-y-4">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <div>
                <h4 className="text-base font-bold text-white flex items-center gap-2">
                  Train #{selectedLog.trainId} Log Entry
                </h4>
                <p className="text-xs text-slate-400">
                  {selectedLog.scheduledDate} at {selectedLog.scheduledTime}
                </p>
              </div>
              <button
                onClick={() => setSelectedLog(null)}
                className="text-slate-400 hover:text-white text-lg font-bold p-1"
              >
                ✕
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div className="grid grid-cols-2 gap-3 bg-slate-950 p-3 rounded-xl border border-slate-800">
                <div>
                  <span className="text-slate-400 block text-[10px] uppercase">Route</span>
                  <span className="font-semibold text-slate-200">
                    {getStationName(selectedLog.origin)} ➔ {getStationName(selectedLog.destination)}
                  </span>
                </div>
                <div>
                  <span className="text-slate-400 block text-[10px] uppercase">Status</span>
                  <span className={selectedLog.isCancelled ? 'text-rose-400 font-bold' : 'text-emerald-400 font-bold'}>
                    {selectedLog.status}
                  </span>
                </div>
                <div>
                  <span className="text-slate-400 block text-[10px] uppercase">Equipment Classification</span>
                  <span className="font-semibold text-rose-300">{selectedLog.trainType}</span>
                </div>
                <div>
                  <span className="text-slate-400 block text-[10px] uppercase">Log Scraped At</span>
                  <span className="font-mono text-slate-300">{selectedLog.timestamp}</span>
                </div>
              </div>

              <div>
                <span className="text-slate-400 block text-[11px] font-semibold mb-1">
                  Formation & Unit Composition:
                </span>
                <CarriageVisualizer
                  rawUnits={selectedLog.rawUnits}
                  carriageInfo={selectedLog.carriageInfo}
                  trainType={selectedLog.trainType}
                />
              </div>
            </div>

            <div className="pt-2 flex justify-end">
              <button
                onClick={() => setSelectedLog(null)}
                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white text-xs font-semibold rounded-xl border border-slate-700"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
