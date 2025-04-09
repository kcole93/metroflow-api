// src/services/staticDataService.ts
import * as fs from 'fs/promises'
import * as path from 'path'
import Papa from 'papaparse'
import {
  StaticData,
  StaticStopInfo,
  StaticRouteInfo,
  StaticTripInfo
  // Assuming types like normalizeSystemType, toGtfsSystemType, etc. are defined if used
  // Or remove them if not necessary for the core logic now
} from '../types'
import * as dotenv from 'dotenv'
// We still need this map for the final step of linking feeds
import {
  SUBWAY_FEEDS,
  LIRR_FEED,
  MNR_FEED,
  ROUTE_ID_TO_FEED_MAP
} from '../services/mtaService'

dotenv.config()

let staticData: StaticData | null = null
const BASE_DATA_PATH =
  process.env.STATIC_DATA_PATH || './src/assets/gtfs-static'

// Base interface for raw stop time data
interface StopTimeBase {
  trip_id: string
  arrival_time?: string // Optional in GTFS spec, though often present
  departure_time?: string // Optional in GTFS spec, though often present
  stop_id: string
  stop_sequence: string
  // Add other optional fields if they exist in your files
  // pickup_type?: string;
  // drop_off_type?: string;
}

async function parseCsvFile<T extends object>(filePath: string): Promise<T[]> {
  try {
    const fileContent = await fs.readFile(filePath, 'utf8')
    const result = Papa.parse<T>(fileContent, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false // KEEP false
    })
    if (result.errors.length > 0) {
      console.warn(
        `Parsing errors in ${path.basename(filePath)}:`,
        result.errors.slice(0, 5)
      )
    }
    return result.data
  } catch (error) {
    console.error(
      `Error reading/parsing CSV ${path.basename(filePath)}:`,
      error
    )
    throw error // Re-throw after logging
  }
}

// --- Ensure StaticData type only includes needed maps ---
// (Remove tripsBySchedule for now unless you reimplement that logic)
// export interface StaticData {
//     stops: Map<string, StaticStopInfo>;
//     routes: Map<string, StaticRouteInfo>; // Key: SYSTEM-ROUTEID
//     trips: Map<string, StaticTripInfo>;   // Key: trip_id (raw from file)
// }
// ---

// Utility function to build route-to-feed mapping
async function buildRouteFeedMapping(
  trips: StaticTripInfo[]
): Promise<{ [key: string]: string }> {
  const routeFeedMap: { [key: string]: string } = {}

  // First, create a map of route IDs to their system type
  const routeSystemMap = new Map<string, string>()
  trips.forEach((trip) => {
    if (trip.route_id && trip.system) {
      routeSystemMap.set(trip.route_id, trip.system)
    }
  })

  // Then create the mapping based on system type
  routeSystemMap.forEach((system, routeId) => {
    switch (system) {
      case 'SUBWAY':
        // For subway, we need to determine which feed based on the route
        const routeLetter = routeId.replace('SUBWAY-', '').toUpperCase()
        if ('ACE'.includes(routeLetter)) {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.ACE
        } else if ('BDFM'.includes(routeLetter)) {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.BDFM
        } else if (routeLetter === 'G') {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.G
        } else if ('JZ'.includes(routeLetter)) {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.JZ
        } else if ('NQRW'.includes(routeLetter)) {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.NQRW
        } else if (routeLetter === 'L') {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.L
        } else if (routeLetter === 'SI') {
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.SI
        } else if (!isNaN(parseInt(routeLetter))) {
          // Numeric routes
          routeFeedMap[`SUBWAY-${routeLetter}`] = SUBWAY_FEEDS.NUMERIC
        }
        break
      case 'LIRR':
        // All LIRR routes use the same feed
        routeFeedMap[`LIRR-${routeId.replace('LIRR-', '')}`] = LIRR_FEED
        break
      case 'MNR':
        // All MNR routes use the same feed
        routeFeedMap[`MNR-${routeId.replace('MNR-', '')}`] = MNR_FEED
        break
    }
  })

  return routeFeedMap
}

export async function loadStaticData(): Promise<StaticData> {
  if (staticData) {
    return staticData
  }

  // --- Define System Names and Paths Consistently (Use Title Case) ---
  const systems = [
    { name: 'LIRR' as const, path: path.join(BASE_DATA_PATH, 'LIRR') },
    { name: 'SUBWAY' as const, path: path.join(BASE_DATA_PATH, 'NYCT') },
    { name: 'MNR' as const, path: path.join(BASE_DATA_PATH, 'MNR') }
  ]
  // ---

  console.log(`Loading static GTFS data...`)

  try {
    // --- Load All Files Concurrently ---
    const promises = systems.flatMap((sys) => [
      parseCsvFile<any>(path.join(sys.path, 'stops.txt')),
      parseCsvFile<any>(path.join(sys.path, 'routes.txt')),
      parseCsvFile<any>(path.join(sys.path, 'trips.txt')),
      parseCsvFile<StopTimeBase>(path.join(sys.path, 'stop_times.txt'))
    ])
    const results = await Promise.all(promises)

    // Deconstruct results based on the order in promises array
    const [
      lirrStopsRaw,
      lirrRoutesRaw,
      lirrTripsRaw,
      lirrStopTimesRaw,
      subwayStopsRaw,
      subwayRoutesRaw,
      subwayTripsRaw,
      subwayStopTimesRaw,
      mnrStopsRaw,
      mnrRoutesRaw,
      mnrTripsRaw,
      mnrStopTimesRaw
    ] = results
    // --- End File Loading ---

    // --- Build tempRoutes map using unique SYSTEM-ROUTEID key ---
    console.log('Building tempRoutes map (Key: System-RouteID)...')
    const tempRoutes = new Map<string, StaticRouteInfo>()
    const addRouteToMap = (r: any, system: StaticStopInfo['system']) => {
      const routeId = r.route_id?.trim()
      if (!routeId) return
      const uniqueKey = `${system}-${routeId}`
      // Ensure route_short_name and route_long_name are included
      tempRoutes.set(uniqueKey, {
        ...r,
        route_id: routeId,
        route_short_name: r.route_short_name?.trim() || '',
        route_long_name: r.route_long_name?.trim() || '',
        system: system
      })
    }
    lirrRoutesRaw.forEach((r) => addRouteToMap(r, 'LIRR'))
    subwayRoutesRaw.forEach((r) => addRouteToMap(r, 'SUBWAY')) // Use Title Case
    mnrRoutesRaw.forEach((r) => addRouteToMap(r, 'MNR'))
    console.log(`Finished building tempRoutes map. Size: ${tempRoutes.size}`)

    // --- Build tempTrips map (Key: raw trip_id) ---
    // Store the system on the trip object itself for later reference
    console.log('Building tempTrips map (Key: trip_id)...')
    const tempTrips = new Map<string, StaticTripInfo>()
    const addTripToMap = (t: any, system: StaticStopInfo['system']) => {
      const tripId = t.trip_id?.trim()
      const routeId = t.route_id?.trim()
      if (!tripId || !routeId) return // Need both IDs
      // --- Parse direction_id ---
      let directionIdNum: number | null = null
      if (
        t.direction_id !== undefined &&
        t.direction_id !== null &&
        t.direction_id !== ''
      ) {
        const parsed = parseInt(t.direction_id, 10)
        if (!isNaN(parsed)) {
          directionIdNum = parsed // Should be 0 or 1
        } else {
          console.warn(
            `[Static Data] Invalid direction_id "${t.direction_id}" for trip ${tripId}`
          )
        }
      }
      // ---

      // Find destinationStopId (needs tripDestinations map built first)
      const destStopId = tripDestinations.get(tripId) || null
      tempTrips.set(tripId, {
        ...t,
        trip_id: tripId,
        route_id: routeId,
        system: system, // Add system to trip info
        destinationStopId: destStopId, // Add destination
        direction_id: directionIdNum // Add direction
      })
    }
    // Note: Destination pass needs to happen before this map is finalized if needed here

    // --- Pass 1: Process stop_times to find trip destinations ---
    // (This needs to run before finalizing tempTrips if destStopId is stored on trip)
    console.log(
      'Pass 1: Processing stop_times to determine trip destinations...'
    )
    const tripDestinations = new Map<string, string>()
    const tripMaxSequence = new Map<string, number>()
    const findDestinations = (stopTimes: StopTimeBase[]) => {
      for (const st of stopTimes) {
        const tripId = st.trip_id?.trim() // Trim here too!
        if (!tripId || st.stop_sequence == null) continue
        const stopSequence = parseInt(st.stop_sequence, 10)
        if (!isNaN(stopSequence)) {
          const currentMax = tripMaxSequence.get(tripId) ?? -1
          if (stopSequence > currentMax) {
            tripMaxSequence.set(tripId, stopSequence)
            tripDestinations.set(tripId, st.stop_id)
          }
        }
      }
    }
    findDestinations(lirrStopTimesRaw)
    findDestinations(subwayStopTimesRaw)
    findDestinations(mnrStopTimesRaw)
    console.log(
      `Pass 1 finished. Found destinations for ${tripDestinations.size} trips.`
    )

    // --- Finalize tempTrips map (includes system and destinationStopId) ---
    console.log('Finalizing tempTrips map...')
    lirrTripsRaw.forEach((t) => addTripToMap(t, 'LIRR'))
    subwayTripsRaw.forEach((t) => addTripToMap(t, 'SUBWAY')) // Use Title Case
    mnrTripsRaw.forEach((t) => addTripToMap(t, 'MNR'))
    console.log(
      `Finished building final tempTrips map. Size: ${tempTrips.size}`
    )

    // --- Pass 2: Build enrichedStops map ---
    console.log(
      'Pass 2: Processing raw stops into enriched map (Key: System-StopId)...'
    )
    const enrichedStops = new Map<string, StaticStopInfo>()
    const processStop = (rawStop: any, system: StaticStopInfo['system']) => {
      const originalStopId = rawStop.stop_id?.trim()
      if (!originalStopId) return

      const uniqueStopId = `${system}-${originalStopId}`

      let locationTypeNum: number | null = null
      const locStr = rawStop.location_type
      if (typeof locStr === 'string' && locStr.trim() !== '') {
        const p = parseInt(locStr, 10)
        if (!isNaN(p)) locationTypeNum = p
      }
      const lat =
        typeof rawStop.stop_lat === 'string'
          ? parseFloat(rawStop.stop_lat)
          : rawStop.stop_lat
      const lon =
        typeof rawStop.stop_lon === 'string'
          ? parseFloat(rawStop.stop_lon)
          : rawStop.stop_lon
      const originalParentId = rawStop.parent_station?.trim() || null
      const uniqueParentId = originalParentId
        ? `${system}-${originalParentId}`
        : null
      if (!enrichedStops.has(uniqueStopId)) {
        enrichedStops.set(uniqueStopId, {
          id: uniqueStopId,
          originalStopId: originalStopId,
          name: rawStop.stop_name || 'Unnamed Stop',
          latitude: !isNaN(lat) ? lat : undefined,
          longitude: !isNaN(lon) ? lon : undefined,
          parentStationId: uniqueParentId,
          locationType: locationTypeNum,
          childStopIds: new Set<string>(),
          servedByRouteIds: new Set<string>(),
          feedUrls: new Set<string>(),
          system: system
        })
      }
    }
    lirrStopsRaw.forEach((s) => processStop(s, 'LIRR'))
    subwayStopsRaw.forEach((s) => processStop(s, 'SUBWAY')) // Use Title Case
    mnrStopsRaw.forEach((s) => processStop(s, 'MNR'))
    console.log(`Pass 2 finished. enrichedStops size: ${enrichedStops.size}`)

    // --- Pass 3: Link children to parents ---
    console.log('Pass 3: Linking child stops to parent stations...')
    let linkedChildrenCount = 0
    for (const [childKey, stopInfo] of enrichedStops.entries()) {
      if (stopInfo.parentStationId) {
        const parentStopInfo = enrichedStops.get(stopInfo.parentStationId) // Direct lookup by parent ID
        if (parentStopInfo) {
          parentStopInfo.childStopIds.add(stopInfo.originalStopId)
          linkedChildrenCount++
        }
      }
    }
    console.log(`Pass 3 finished. Linked ${linkedChildrenCount} children.`)

    // --- Pass 4: Process stop_times to link routes/feeds ---
    console.log('Pass 4: Processing stop times to link routes/feeds...')
    const processStopTimes = (stopTimes: StopTimeBase[]) => {
      let linksMade = 0
      let checkedCount = 0

      for (const st of stopTimes) {
        checkedCount++
        const stopTimeTripId = st.trip_id?.trim()
        if (!stopTimeTripId) continue

        const trip = tempTrips.get(stopTimeTripId)
        if (!trip || !trip.route_id || !trip.system) continue

        const routeMapKey = `${trip.system}-${trip.route_id}`
        const route = tempRoutes.get(routeMapKey)
        if (!route) continue

        const originalStopId = st.stop_id?.trim()
        if (!originalStopId) continue
        const childStopKey = `${trip.system}-${originalStopId}` // Key for the stop in stop_times
        const childStopInfo = enrichedStops.get(childStopKey)
        if (!childStopInfo) continue // Skip if stop_time references unknown stop for this system

        const routeId = route.route_id
        const routeSystem = route.system
        const feedKey = `${routeSystem}-${routeId}`
        const feedUrl = ROUTE_ID_TO_FEED_MAP[feedKey]

        if (feedUrl) {
          let addedLink = false
          if (!childStopInfo.feedUrls.has(feedUrl)) {
            childStopInfo.feedUrls.add(feedUrl)
            addedLink = true
          }
          if (!childStopInfo.servedByRouteIds.has(routeId)) {
            childStopInfo.servedByRouteIds.add(routeId)
            addedLink = true
          }

          if (childStopInfo.parentStationId) {
            const parentStopInfo = enrichedStops.get(
              childStopInfo.parentStationId
            )
            if (parentStopInfo) {
              if (!parentStopInfo.feedUrls.has(feedUrl)) {
                parentStopInfo.feedUrls.add(feedUrl)
              }
              if (!parentStopInfo.servedByRouteIds.has(routeId)) {
                parentStopInfo.servedByRouteIds.add(routeId)
              }
            }
          }
          if (addedLink) linksMade++
        }
      }
      console.log(
        `Finished processing ${stopTimes.length} stop times. Links added/updated: ${linksMade}`
      )
    }

    processStopTimes([
      ...lirrStopTimesRaw,
      ...subwayStopTimesRaw,
      ...mnrStopTimesRaw
    ])
    console.log('Pass 4 finished.')

    // After loading trips, build the route feed mapping
    const routeFeedMap = await buildRouteFeedMapping([...tempTrips.values()])

    // Add the route feed mapping to the static data
    staticData = {
      stops: enrichedStops,
      routes: tempRoutes,
      trips: tempTrips,
      routeFeedMap: new Map(
        Object.entries(routeFeedMap).map(([key, value]) => [key, [value]])
      )
    }

    console.log(
      `Static data loaded: ${enrichedStops.size} total stops processed.`
    )
    return staticData
  } catch (error) {
    console.error('Fatal error loading static GTFS data:', error)
    throw new Error('Could not load essential static GTFS data.')
  }
}

// Modify getStaticData to include the route feed mapping
export async function getStaticData(): Promise<StaticData> {
  if (staticData) return staticData

  try {
    // --- Define System Names and Paths Consistently (Use Title Case) ---
    const systems = [
      { name: 'LIRR' as const, path: path.join(BASE_DATA_PATH, 'LIRR') },
      { name: 'SUBWAY' as const, path: path.join(BASE_DATA_PATH, 'NYCT') },
      { name: 'MNR' as const, path: path.join(BASE_DATA_PATH, 'MNR') }
    ]

    console.log(`Loading static GTFS data...`)

    // --- Load All Files Concurrently ---
    const promises = systems.flatMap((sys) => [
      parseCsvFile<any>(path.join(sys.path, 'stops.txt')),
      parseCsvFile<any>(path.join(sys.path, 'routes.txt')),
      parseCsvFile<any>(path.join(sys.path, 'trips.txt')),
      parseCsvFile<StopTimeBase>(path.join(sys.path, 'stop_times.txt'))
    ])
    const results = await Promise.all(promises)

    // Deconstruct results based on the order in promises array
    const [
      lirrStopsRaw,
      lirrRoutesRaw,
      lirrTripsRaw,
      lirrStopTimesRaw,
      subwayStopsRaw,
      subwayRoutesRaw,
      subwayTripsRaw,
      subwayStopTimesRaw,
      mnrStopsRaw,
      mnrRoutesRaw,
      mnrTripsRaw,
      mnrStopTimesRaw
    ] = results

    // --- Build tempTrips map (Key: raw trip_id) ---
    const tempTrips = new Map<string, StaticTripInfo>()
    const addTripToMap = (t: any, system: StaticStopInfo['system']) => {
      const tripId = t.trip_id?.trim()
      const routeId = t.route_id?.trim()
      if (!tripId || !routeId) return
      tempTrips.set(tripId, {
        trip_id: tripId,
        route_id: routeId,
        system: system,
        service_id: t.service_id?.trim(),
        direction_id: t.direction_id
      })
    }
    lirrTripsRaw.forEach((t) => addTripToMap(t, 'LIRR'))
    subwayTripsRaw.forEach((t) => addTripToMap(t, 'SUBWAY'))
    mnrTripsRaw.forEach((t) => addTripToMap(t, 'MNR'))

    // After loading trips, build the route feed mapping
    const routeFeedMap = await buildRouteFeedMapping([...tempTrips.values()])

    // Add the route feed mapping to the static data
    staticData = {
      stops: new Map(),
      routes: new Map(),
      trips: new Map(),
      routeFeedMap: new Map(
        Object.entries(routeFeedMap).map(([key, value]) => [key, [value]])
      )
    }

    return staticData
  } catch (error) {
    console.error('Error loading static data:', error)
    throw error
  }
}
