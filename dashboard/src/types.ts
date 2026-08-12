export interface RawTrainRow {
  Timestamp: string;
  'Scheduled Date': string;
  'Scheduled Time': string;
  'Train ID': string;
  Status: string;
  Origin: string;
  Destination: string;
  'Train Type Classification': string;
  'Carriage Info': string;
  'Raw Units': string;
}

export interface TrainLog {
  id: string;
  timestamp: string;
  scheduledDate: string;
  scheduledTime: string;
  trainId: string;
  status: 'Scheduled' | 'Cancelled' | string;
  isCancelled: boolean;
  origin: string;
  destination: string;
  trainType: string;
  carriageInfo: string;
  carCount: number;
  rawUnits: string;
  unitList: string[];
}

export interface EquipmentBreakdown {
  [category: string]: number;
}

export interface TrainStats {
  trainId: string;
  totalLogs: number;
  scheduledCount: number;
  cancelledCount: number;
  cancellationRate: number; // percentage
  primaryType: string;
  primaryTypePercentage: number;
  equipmentDistribution: EquipmentBreakdown;
  primaryRoute: string;
  routes: { routeStr: string; count: number }[];
  typicalScheduledTime: string;
  avgCarCount: number;
  mostCommonCarCount: number;
  carCountDistribution: { cars: string; count: number }[];
  unitConfigurations: { config: string; count: number }[];
  firstSeenDate: string;
  lastSeenDate: string;
}

export interface CorridorStats {
  totalLogs: number;
  uniqueTrainIds: number;
  totalCancelled: number;
  overallCancellationRate: number;
  equipmentTotals: EquipmentBreakdown;
  topTalgoTrainIds: string[];
  mostFrequentTrainIds: string[];
  highestCancellationTrainIds: { trainId: string; rate: number; count: number }[];
}
