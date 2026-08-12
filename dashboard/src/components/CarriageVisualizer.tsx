import React from 'react';

interface CarriageVisualizerProps {
  rawUnits: string;
  carriageInfo?: string;
  trainType?: string;
}

export const CarriageVisualizer: React.FC<CarriageVisualizerProps> = ({
  rawUnits,
  carriageInfo,
  trainType = 'Unknown',
}) => {
  if (!rawUnits) {
    return (
      <div className="text-xs text-slate-400 italic">No detailed unit configuration logged</div>
    );
  }

  const units = rawUnits.split('+').map((u) => u.trim()).filter(Boolean);

  const getUnitBadgeColor = (unit: string) => {
    const u = unit.toUpperCase();
    if (u === 'EB') return 'bg-amber-500/20 text-amber-300 border-amber-500/40 font-bold'; // Siemens Vectron loco
    if (u.startsWith('BPD')) return 'bg-rose-500/20 text-rose-300 border-rose-500/40'; // Cab/Service car
    if (u.startsWith('APT') || u.startsWith('AP') || u.startsWith('AV'))
      return 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40'; // 1st Class
    if (u.startsWith('BP') || u.startsWith('BV'))
      return 'bg-sky-500/20 text-sky-300 border-sky-500/40'; // 2nd Class
    if (u.startsWith('MFU')) return 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'; // IC3 motor
    return 'bg-slate-700 text-slate-300 border-slate-600';
  };

  const getUnitTooltip = (unit: string) => {
    const u = unit.toUpperCase();
    if (u === 'EB') return 'DSB EB / Vectron Electric Locomotive';
    if (u === 'BPD') return 'Talgo End Car w/ Driver Cab & Service Area';
    if (u === 'APT') return 'Talgo 1st Class PRM/Bistro Car';
    if (u === 'AP') return 'Talgo 1st Class Passenger Car';
    if (u === 'BPH') return 'Talgo 2nd Class PRM Car';
    if (u === 'BPT') return 'Talgo 2nd Class PRM/Bistro Car';
    if (u === 'BP') return 'Talgo 2nd Class Passenger Car';
    if (u === 'MFU') return 'IC3 Diesel Motor Unit Car';
    if (u === 'AV') return 'DB 1st Class EuroCity Coach';
    if (u === 'BV' || u === 'BVS' || u === 'BPB' || u === 'BPX') return 'DB 2nd Class EuroCity Coach';
    return `Unit Code: ${unit}`;
  };

  return (
    <div className="space-y-2">
      {carriageInfo && (
        <div className="flex items-center justify-between text-xs text-slate-400">
          <span className="font-medium text-slate-300">{carriageInfo}</span>
          <span className="text-[11px] px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
            {units.length} Units Connected
          </span>
        </div>
      )}

      {/* Visual train formation layout */}
      <div className="flex items-center gap-1 overflow-x-auto pb-2 scrollbar-thin scrollbar-thumb-slate-700">
        {units.map((unit, i) => {
          const isLoco = unit.toUpperCase() === 'EB';
          return (
            <React.Fragment key={i}>
              <div
                title={getUnitTooltip(unit)}
                className={`flex-shrink-0 flex flex-col items-center justify-center px-2 py-1 rounded text-[10px] border transition-all hover:scale-105 cursor-help ${getUnitBadgeColor(
                  unit
                )} ${isLoco ? 'min-w-[40px] shadow-sm shadow-amber-500/10' : 'min-w-[32px]'}`}
              >
                <span className="font-mono font-semibold">{unit}</span>
                <span className="text-[8px] opacity-70">
                  {isLoco ? 'LOCO' : i === 0 || i === units.length - 1 ? 'END' : `CAR ${i}`}
                </span>
              </div>
              {i < units.length - 1 && (
                <span className="text-slate-600 text-[10px] flex-shrink-0">•</span>
              )}
            </React.Fragment>
          );
        })}
      </div>
    </div>
  );
};
