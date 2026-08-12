import Papa from 'papaparse';
import { RawTrainRow, TrainLog, TrainStats, CorridorStats, EquipmentBreakdown } from '../types';

export const STATION_NAMES: Record<string, string> = {
  KH: 'København H (Copenhagen)',
  HMB: 'Hamburg Hbf',
  AP: 'Aarhus H',
  AHAR: 'Aarhus H',
  FA: 'Fredericia',
  KD: 'Kolding',
  PA: 'Padborg',
  AA: 'Aalborg',
  CPH: 'Københavns Lufthavn',
  FLB: 'Flensburg',
  PAGR: 'Padborg Grænse',
  AFW: 'Flensburg Weiche',
  '#PH': 'København PH',
};

export const CATEGORY_COLORS: Record<string, string> = {
  Talgo: '#e63946',
  Railjet: '#005baa',
  IC3: '#457b9d',
  'German IC Coaches': '#1d3557',
  'Vectron-hauled': '#2a9d8f',
  Cancelled: '#9e9e9e',
  Unknown: '#f4a261',
};

export function getStationName(code: string): string {
  if (!code) return 'Unknown';
  return STATION_NAMES[code] || code;
}

export function parseCarCount(carriageInfo: string): number {
  if (!carriageInfo) return 0;
  const match = carriageInfo.match(/(\d+)\s*cars/i);
  return match ? parseInt(match[1], 10) : 0;
}

export function parseTrainLogs(csvText: string): TrainLog[] {
  const parsed = Papa.parse<RawTrainRow>(csvText, {
    header: true,
    delimiter: ';',
    skipEmptyLines: true,
  });

  const logs: TrainLog[] = [];

  parsed.data.forEach((row, index) => {
    const trainId = row['Train ID'] ? String(row['Train ID']).trim() : '';
    if (!trainId) return;

    const status = row['Status'] || 'Scheduled';
    const isCancelled = status.toLowerCase().includes('cancel');
    const carriageInfo = row['Carriage Info'] || '';
    const carCount = parseCarCount(carriageInfo);
    const rawUnits = row['Raw Units'] || '';
    const unitList = rawUnits ? rawUnits.split('+').map((u) => u.trim()).filter(Boolean) : [];

    logs.push({
      id: `log-${index}`,
      timestamp: row['Timestamp'] || '',
      scheduledDate: row['Scheduled Date'] || '',
      scheduledTime: row['Scheduled Time'] || '',
      trainId,
      status,
      isCancelled,
      origin: row['Origin'] || '',
      destination: row['Destination'] || '',
      trainType: row['Train Type Classification'] || 'Unknown',
      carriageInfo,
      carCount,
      rawUnits,
      unitList,
    });
  });

  return logs;
}

export function computeCorridorStats(logs: TrainLog[]): CorridorStats {
  const totalLogs = logs.length;
  const trainIdsSet = new Set<string>();
  let totalCancelled = 0;
  const equipmentTotals: EquipmentBreakdown = {};
  const trainIdLogsMap: Record<string, TrainLog[]> = {};

  logs.forEach((log) => {
    trainIdsSet.add(log.trainId);
    if (!trainIdLogsMap[log.trainId]) trainIdLogsMap[log.trainId] = [];
    trainIdLogsMap[log.trainId].push(log);

    if (log.isCancelled) {
      totalCancelled++;
      equipmentTotals['Cancelled'] = (equipmentTotals['Cancelled'] || 0) + 1;
    } else {
      const type = log.trainType || 'Unknown';
      equipmentTotals[type] = (equipmentTotals[type] || 0) + 1;
    }
  });

  // Calculate cancellation rate per train ID
  const trainStatsList: { trainId: string; rate: number; count: number; talgoCount: number }[] = [];
  Object.entries(trainIdLogsMap).forEach(([trainId, tLogs]) => {
    const cancelled = tLogs.filter((l) => l.isCancelled).length;
    const talgo = tLogs.filter((l) => !l.isCancelled && l.trainType === 'Talgo').length;
    trainStatsList.push({
      trainId,
      rate: (cancelled / tLogs.length) * 100,
      count: tLogs.length,
      talgoCount: talgo,
    });
  });

  // Top Talgo trains
  const topTalgoTrainIds = [...trainStatsList]
    .sort((a, b) => b.talgoCount - a.talgoCount)
    .slice(0, 5)
    .map((t) => t.trainId);

  // Most frequent trains
  const mostFrequentTrainIds = [...trainStatsList]
    .sort((a, b) => b.count - a.count)
    .slice(0, 8)
    .map((t) => t.trainId);

  // Highest cancellation rate (for trains with at least 10 logs)
  const highestCancellationTrainIds = [...trainStatsList]
    .filter((t) => t.count >= 10)
    .sort((a, b) => b.rate - a.rate)
    .slice(0, 5)
    .map((t) => ({ trainId: t.trainId, rate: Math.round(t.rate), count: t.count }));

  return {
    totalLogs,
    uniqueTrainIds: trainIdsSet.size,
    totalCancelled,
    overallCancellationRate: totalLogs > 0 ? Math.round((totalCancelled / totalLogs) * 1000) / 10 : 0,
    equipmentTotals,
    topTalgoTrainIds,
    mostFrequentTrainIds,
    highestCancellationTrainIds,
  };
}

export function computeTrainStats(trainId: string, logs: TrainLog[]): TrainStats {
  const trainLogs = logs.filter((l) => l.trainId === trainId);
  const totalLogs = trainLogs.length;

  if (totalLogs === 0) {
    return {
      trainId,
      totalLogs: 0,
      scheduledCount: 0,
      cancelledCount: 0,
      cancellationRate: 0,
      primaryType: 'N/A',
      primaryTypePercentage: 0,
      equipmentDistribution: {},
      primaryRoute: 'N/A',
      routes: [],
      typicalScheduledTime: 'N/A',
      avgCarCount: 0,
      mostCommonCarCount: 0,
      carCountDistribution: [],
      unitConfigurations: [],
      firstSeenDate: 'N/A',
      lastSeenDate: 'N/A',
    };
  }

  let cancelledCount = 0;
  const equipmentDistribution: EquipmentBreakdown = {};
  const typeCounts: Record<string, number> = {};
  const routeCounts: Record<string, number> = {};
  const timeCounts: Record<string, number> = {};
  const carCountMap: Record<number, number> = {};
  const unitConfigMap: Record<string, number> = {};
  let sumCars = 0;
  let carCountValid = 0;

  trainLogs.forEach((log) => {
    if (log.isCancelled) {
      cancelledCount++;
      equipmentDistribution['Cancelled'] = (equipmentDistribution['Cancelled'] || 0) + 1;
    } else {
      const type = log.trainType || 'Unknown';
      equipmentDistribution[type] = (equipmentDistribution[type] || 0) + 1;
      typeCounts[type] = (typeCounts[type] || 0) + 1;
    }

    const routeStr = `${log.origin || '?'} ➔ ${log.destination || '?'}`;
    routeCounts[routeStr] = (routeCounts[routeStr] || 0) + 1;

    if (log.scheduledTime) {
      const shortTime = log.scheduledTime.slice(0, 5);
      timeCounts[shortTime] = (timeCounts[shortTime] || 0) + 1;
    }

    if (log.carCount > 0) {
      carCountMap[log.carCount] = (carCountMap[log.carCount] || 0) + 1;
      sumCars += log.carCount;
      carCountValid++;
    }

    if (log.rawUnits) {
      unitConfigMap[log.rawUnits] = (unitConfigMap[log.rawUnits] || 0) + 1;
    }
  });

  // Primary equipment
  let primaryType = 'Unknown';
  let primaryTypeCount = 0;
  Object.entries(typeCounts).forEach(([type, count]) => {
    if (count > primaryTypeCount) {
      primaryType = type;
      primaryTypeCount = count;
    }
  });

  const scheduledCount = totalLogs - cancelledCount;
  const primaryTypePercentage = scheduledCount > 0 ? Math.round((primaryTypeCount / scheduledCount) * 100) : 0;
  const cancellationRate = Math.round((cancelledCount / totalLogs) * 1000) / 10;

  // Primary route
  let primaryRoute = 'N/A';
  let maxRouteCount = 0;
  const routesList = Object.entries(routeCounts)
    .map(([routeStr, count]) => ({ routeStr, count }))
    .sort((a, b) => b.count - a.count);

  if (routesList.length > 0) {
    primaryRoute = routesList[0].routeStr;
  }

  // Typical scheduled time
  let typicalScheduledTime = 'N/A';
  let maxTimeCount = 0;
  Object.entries(timeCounts).forEach(([time, count]) => {
    if (count > maxTimeCount) {
      typicalScheduledTime = time;
      maxTimeCount = count;
    }
  });

  // Car counts
  const avgCarCount = carCountValid > 0 ? Math.round((sumCars / carCountValid) * 10) / 10 : 0;
  let mostCommonCarCount = 0;
  let maxCarCountFreq = 0;
  const carCountDistribution = Object.entries(carCountMap)
    .map(([cars, count]) => {
      const c = parseInt(cars, 10);
      if (count > maxCarCountFreq) {
        maxCarCountFreq = count;
        mostCommonCarCount = c;
      }
      return { cars: `${cars} cars`, count };
    })
    .sort((a, b) => b.count - a.count);

  // Unit configurations
  const unitConfigurations = Object.entries(unitConfigMap)
    .map(([config, count]) => ({ config, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);

  const dates = trainLogs.map((l) => l.scheduledDate).filter(Boolean);

  return {
    trainId,
    totalLogs,
    scheduledCount,
    cancelledCount,
    cancellationRate,
    primaryType,
    primaryTypePercentage,
    equipmentDistribution,
    primaryRoute,
    routes: routesList,
    typicalScheduledTime,
    avgCarCount,
    mostCommonCarCount,
    carCountDistribution,
    unitConfigurations,
    firstSeenDate: dates.length > 0 ? dates[0] : 'N/A',
    lastSeenDate: dates.length > 0 ? dates[dates.length - 1] : 'N/A',
  };
}
