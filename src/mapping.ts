import { DataSubmitted, DataGroupHeartBeat } from "../generated/PropertyDataConsensus/PropertyDataConsensus"
import { Property, PropertyLabelPair, LabelCounter, PropertySubmitterPair, SubmitterLabelCounter, SubmitterCountyPair, SubmitterCountyCounter, CountyCounter, SubmitterCountyLabelPair, SubmitterCountyLabelCounter } from "../generated/schema"
import { ipfs, json, Bytes, log, BigInt, BigDecimal } from "@graphprotocol/graph-ts"

// Base32 RFC4648 alphabet (lowercase variant used by CID)
const BASE32_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";

// Function to encode bytes to base32 (RFC4648 implementation)
function encodeBase32(data: Uint8Array): string {
  let output = "";
  let bits = 0;
  let value = 0;
  
  for (let i = 0; i < data.length; i++) {
    value = (value << 8) | data[i];
    bits += 8;
    
    while (bits >= 5) {
      output += BASE32_ALPHABET[(value >>> (bits - 5)) & 0x1f];
      bits -= 5;
    }
  }
  
  if (bits > 0) {
    output += BASE32_ALPHABET[(value << (5 - bits)) & 0x1f];
  }
  
  return output;
}

// Function to fetch from IPFS with retry logic
function fetchFromIPFSWithRetry(cid: string, maxRetries: i32 = 3): Bytes | null {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    log.info("IPFS fetch attempt {} of {} for CID: {}", [attempt.toString(), maxRetries.toString(), cid]);
    
    let result = ipfs.cat(cid);
    if (result) {
      log.info("✅ IPFS fetch successful on attempt {} for CID: {}", [attempt.toString(), cid]);
      return result;
    }
    
    log.warning("⚠️ IPFS fetch failed on attempt {} for CID: {}", [attempt.toString(), cid]);
    
    // Small delay between retries (The Graph handles this internally)
    // We can't implement actual delay in AssemblyScript, but The Graph
    // may naturally have some delay between our calls
  }
  
  log.error("❌ IPFS fetch failed after {} attempts for CID: {}", [maxRetries.toString(), cid]);
  return null;
}

// Function to implement your CLI's cid-decode logic
function bytes32ToCID(dataHash: Bytes): string {
  // Step 1: hashBytes are already the SHA-256 digest (from ethers.getBytes(input))
  let hashBytes = dataHash;
  
  // Step 2: Create SHA-256 multihash (Digest.create(sha256.code, hashBytes))
  // SHA-256 code is 0x12, length is 32 (0x20)
  let multihash = new Uint8Array(34); // 2 bytes header + 32 bytes hash
  multihash[0] = 0x12; // sha256.code
  multihash[1] = 0x20; // 32 bytes length
  
  // Copy hash bytes
  for (let i = 0; i < 32; i++) {
    multihash[i + 2] = hashBytes[i];
  }
  
  // Step 3: Create CID v1 with raw codec (CID.create(1, 0x55, multihash))
  let cidData = new Uint8Array(36); // 1 byte version + 1 byte codec + 34 bytes multihash
  cidData[0] = 0x01; // CID version 1
  cidData[1] = 0x55; // raw codec (0x55)
  
  // Copy multihash
  for (let i = 0; i < 34; i++) {
    cidData[i + 2] = multihash[i];
  }
  
  // Step 4: Convert to base32 string (cid.toString())
  let base32String = encodeBase32(cidData);
  
  // Add 'b' prefix for base32 encoding
  return "b" + base32String;
}

// Function to extract county from propertyHash IPFS chain
// propertyHash -> property_seed -> to -> county_jurisdiction
function extractCountyFromPropertyHash(propertyHashHex: string): string | null {
  log.info("🏠 Starting county extraction for propertyHash: {}", [propertyHashHex])
  
  // Convert hex propertyHash to CID
  let propertyHashBytes = Bytes.fromHexString(propertyHashHex)
  let propertyCID = bytes32ToCID(propertyHashBytes)
  
  // Step 1: Fetch property data from propertyHash CID
  let propertyData = fetchFromIPFSWithRetry(propertyCID, 3)
  if (!propertyData) {
    log.warning("⚠️ Failed to fetch property data for propertyHash: {}", [propertyHashHex])
    return null
  }
  
  // Parse property JSON to get property_seed CID
  let propertyJson = json.try_fromBytes(propertyData)
  if (propertyJson.isError) {
    log.warning("⚠️ Failed to parse property JSON for propertyHash: {}", [propertyHashHex])
    return null
  }
  
  let propertyObject = propertyJson.value.toObject()
  let relationshipsValue = propertyObject.get("relationships")
  if (!relationshipsValue || relationshipsValue.isNull()) {
    log.warning("⚠️ No relationships found in property data for propertyHash: {}", [propertyHashHex])
    return null
  }
  
  let relationshipsObject = relationshipsValue.toObject()
  let propertySeedValue = relationshipsObject.get("property_seed")
  if (!propertySeedValue || propertySeedValue.isNull()) {
    log.warning("⚠️ No property_seed found in relationships for propertyHash: {}", [propertyHashHex])
    return null
  }
  
  let propertySeedObject = propertySeedValue.toObject()
  let propertySeedCidValue = propertySeedObject.get("/")
  if (!propertySeedCidValue || propertySeedCidValue.isNull()) {
    log.warning("⚠️ No property_seed CID found for propertyHash: {}", [propertyHashHex])
    return null
  }
  
  let propertySeedCID = propertySeedCidValue.toString()
  log.info("📍 Found property_seed CID: {} for propertyHash: {}", [propertySeedCID, propertyHashHex])
  
  // Step 2: Fetch property_seed data
  let propertySeedData = fetchFromIPFSWithRetry(propertySeedCID, 3)
  if (!propertySeedData) {
    log.warning("⚠️ Failed to fetch property_seed data for CID: {}", [propertySeedCID])
    return null
  }
  
  // Parse property_seed JSON to get "to" CID
  let propertySeedJson = json.try_fromBytes(propertySeedData)
  if (propertySeedJson.isError) {
    log.warning("⚠️ Failed to parse property_seed JSON for CID: {}", [propertySeedCID])
    return null
  }
  
  let propertySeedObject2 = propertySeedJson.value.toObject()
  let toValue = propertySeedObject2.get("to")
  if (!toValue || toValue.isNull()) {
    log.warning("⚠️ No 'to' field found in property_seed data for CID: {}", [propertySeedCID])
    return null
  }
  
  let toObject = toValue.toObject()
  let toCidValue = toObject.get("/")
  if (!toCidValue || toCidValue.isNull()) {
    log.warning("⚠️ No 'to' CID found for property_seed CID: {}", [propertySeedCID])
    return null
  }
  
  let toCID = toCidValue.toString()
  log.info("🎯 Found 'to' CID: {} for property_seed CID: {}", [toCID, propertySeedCID])
  
  // Step 3: Fetch final data containing county_jurisdiction
  let finalData = fetchFromIPFSWithRetry(toCID, 3)
  if (!finalData) {
    log.warning("⚠️ Failed to fetch final data for CID: {}", [toCID])
    return null
  }
  
  // Parse final JSON to get county_jurisdiction
  let finalJson = json.try_fromBytes(finalData)
  if (finalJson.isError) {
    log.warning("⚠️ Failed to parse final JSON for CID: {}", [toCID])
    return null
  }
  
  let finalObject = finalJson.value.toObject()
  let countyValue = finalObject.get("county_jurisdiction")
  if (!countyValue || countyValue.isNull()) {
    log.warning("⚠️ No county_jurisdiction found in final data for CID: {}", [toCID])
    return null
  }
  
  let county = countyValue.toString()
  log.info("✅ Successfully extracted county: '{}' for propertyHash: {}", [county, propertyHashHex])
  return county
}

export function handleDataSubmitted(event: DataSubmitted): void {
  // Create a unique ID using transaction hash and log index
  let id = event.transaction.hash.toHexString() + "-" + event.logIndex.toString()
  
  let property = new Property(id)
  
  // Set property fields from event parameters
  property.propertyHash = event.params.propertyHash.toHexString()
  property.dataGroupHash = event.params.dataGroupHash.toHexString()
  property.submitter = event.params.submitter.toHexString()
  property.dataHash = event.params.dataHash.toHexString()
  
  // Set owner as the submitter (based on your requirements)
  property.owner = event.params.submitter.toHexString()
  
  // Try to decode dataHash and fetch from IPFS
  // Convert bytes32 dataHash back to CID using CLI logic
  let cid = bytes32ToCID(event.params.dataHash as Bytes)
  
  // Debug logging
  log.info("Processing dataHash: {}, converted to CID: {}", [event.params.dataHash.toHexString(), cid])
  
  // Fetch content from IPFS with retry logic
  let ipfsResult = fetchFromIPFSWithRetry(cid, 3)
  
  if (ipfsResult) {
    log.info("✅ IPFS fetch successful for CID: {}, content length: {}", [cid, ipfsResult.length.toString()])
    property.dataContent = ipfsResult.toString()
    
    // Try to parse as JSON and extract label
    let jsonResult = json.try_fromBytes(ipfsResult)
    if (!jsonResult.isError) {
      let jsonObject = jsonResult.value.toObject()
      let labelValue = jsonObject.get("label")
      
      if (labelValue && !labelValue.isNull()) {
        property.label = labelValue.toString()
        log.info("✅ Successfully extracted label: '{}' for propertyHash: {}", [property.label!, property.propertyHash])
      } else {
        log.warning("⚠️ Label field not found in JSON for CID: {}", [cid])
        property.label = null
      }
    } else {
      log.error("❌ Failed to parse JSON from IPFS content for CID: {}", [cid])
      property.label = null
    }
  } else {
    // IPFS fetch failed - likely timeout
    log.error("❌ IPFS TIMEOUT: Failed to fetch CID: {} - Content exists but The Graph's IPFS node timed out", [cid])
    
    // Store the timeout info for debugging
    property.dataContent = "TIMEOUT:" + cid
    property.label = "TIMEOUT"
    
    // This allows you to identify timeouts in queries and potentially process them externally
  }
  
  // Extract county from propertyHash IPFS chain
  let county = extractCountyFromPropertyHash(property.propertyHash)
  if (county) {
    property.county = county
    log.info("✅ County extracted: '{}' for propertyHash: {}", [county, property.propertyHash])
  } else {
    property.county = null
    log.warning("⚠️ Failed to extract county for propertyHash: {}", [property.propertyHash])
  }
  
  // Set block and transaction information
  property.timestamp = event.block.timestamp
  property.blockNumber = event.block.number
  property.transactionHash = event.transaction.hash.toHexString()
  
  // Save the entity
  property.save()
  
  // Track county information for mining leaderboard (if county extracted successfully)
  if (property.county && property.county!.length > 0) {
    // Track unique (submitter, county) pairs for leaderboard
    let submitterCountyPairId = property.submitter + "-" + property.county!
    let submitterCountyPair = SubmitterCountyPair.load(submitterCountyPairId)
    
    if (!submitterCountyPair) {
      submitterCountyPair = new SubmitterCountyPair(submitterCountyPairId)
      submitterCountyPair.submitter = property.submitter
      submitterCountyPair.county = property.county!
      submitterCountyPair.firstSeenTimestamp = event.block.timestamp
      submitterCountyPair.firstSeenBlockNumber = event.block.number
      submitterCountyPair.save()
      
      log.info("New submitter-county pair: {} mined county: {}", [property.submitter, property.county!])
    }
    
    // Track county-label combinations if label exists and is not TIMEOUT
    if (property.label && property.label!.length > 0 && property.label! != "TIMEOUT") {
      let submitterCountyLabelId = property.submitter + "-" + property.county! + "-" + property.label!
      let submitterCountyLabelPair = SubmitterCountyLabelPair.load(submitterCountyLabelId)
      
      if (!submitterCountyLabelPair) {
        submitterCountyLabelPair = new SubmitterCountyLabelPair(submitterCountyLabelId)
        submitterCountyLabelPair.submitter = property.submitter
        submitterCountyLabelPair.county = property.county!
        submitterCountyLabelPair.label = property.label!
        submitterCountyLabelPair.firstSeenTimestamp = event.block.timestamp
        submitterCountyLabelPair.firstSeenBlockNumber = event.block.number
        submitterCountyLabelPair.save()
        
        log.info("New submitter-county-label combination from HeartBeat: {} mined {} in county {}", [
          property.submitter, property.label!, property.county!
        ])
      }
      
      // Update counter for this specific submitter-county-label combination
      let submitterCountyLabelCounter = SubmitterCountyLabelCounter.load(submitterCountyLabelId)
      if (!submitterCountyLabelCounter) {
        submitterCountyLabelCounter = new SubmitterCountyLabelCounter(submitterCountyLabelId)
        submitterCountyLabelCounter.submitter = property.submitter
        submitterCountyLabelCounter.county = property.county!
        submitterCountyLabelCounter.label = property.label!
        submitterCountyLabelCounter.propertiesMined = 0
        submitterCountyLabelCounter.lastActivityTimestamp = event.block.timestamp
        submitterCountyLabelCounter.lastActivityBlockNumber = event.block.number
      }
      
      submitterCountyLabelCounter.propertiesMined = submitterCountyLabelCounter.propertiesMined + 1
      submitterCountyLabelCounter.lastActivityTimestamp = event.block.timestamp
      submitterCountyLabelCounter.lastActivityBlockNumber = event.block.number
      submitterCountyLabelCounter.save()
      
      log.info("Updated county-label stats from HeartBeat: {} has {} {} properties in {}", [
        property.submitter,
        submitterCountyLabelCounter.propertiesMined.toString(),
        property.label!,
        property.county!
      ])
    }
    
    // Update submitter's overall county counter (regardless of label)
    let submitterCounter = SubmitterCountyCounter.load(property.submitter)
    if (!submitterCounter) {
      submitterCounter = new SubmitterCountyCounter(property.submitter)
      submitterCounter.submitter = property.submitter
      submitterCounter.uniqueCounties = []
      submitterCounter.uniqueCountyCount = 0
      submitterCounter.totalPropertiesMined = 0
      submitterCounter.firstActivityTimestamp = event.block.timestamp
      submitterCounter.lastActivityTimestamp = event.block.timestamp
      submitterCounter.lastActivityBlockNumber = event.block.number
      submitterCounter.propertiesPerSecond = BigDecimal.fromString("0")
    }
    
    // Add county to list if not already present
    let counties = submitterCounter.uniqueCounties
    let countyExists = false
    for (let i = 0; i < counties.length; i++) {
      if (counties[i] == property.county!) {
        countyExists = true
        break
      }
    }
    
    if (!countyExists) {
      counties.push(property.county!)
      submitterCounter.uniqueCounties = counties
      submitterCounter.uniqueCountyCount = counties.length
    }
    
    submitterCounter.totalPropertiesMined = submitterCounter.totalPropertiesMined + 1
    submitterCounter.lastActivityTimestamp = event.block.timestamp
    submitterCounter.lastActivityBlockNumber = event.block.number
    
    // Calculate mining speed (properties per second)
    let timeElapsed = event.block.timestamp.minus(submitterCounter.firstActivityTimestamp)
    if (timeElapsed.gt(BigInt.fromI32(0))) {
      // timeElapsed is already in seconds, so we can directly calculate
      submitterCounter.propertiesPerSecond = BigInt.fromI32(submitterCounter.totalPropertiesMined).toBigDecimal().div(timeElapsed.toBigDecimal())
    }
    
    submitterCounter.save()
    
    log.info("Updated county stats: {} has {} unique counties, {} total properties, speed: {} props/sec", [
      property.submitter,
      submitterCounter.uniqueCountyCount.toString(),
      submitterCounter.totalPropertiesMined.toString(),
      submitterCounter.propertiesPerSecond.toString()
    ])
    
    // Update county-specific counter
    let countyCounter = CountyCounter.load(property.county!)
    if (!countyCounter) {
      countyCounter = new CountyCounter(property.county!)
      countyCounter.county = property.county!
      countyCounter.uniquePropertiesCount = 0
    }
    
    countyCounter.uniquePropertiesCount = countyCounter.uniquePropertiesCount + 1
    countyCounter.save()
    
    log.info("Mining stats updated - submitter: {} county: {}", [
      property.submitter,
      property.county!
    ])
  }
  
  // Track unique (propertyHash, label) pairs if label exists and not TIMEOUT
  if (property.label && property.label!.length > 0 && property.label! != "TIMEOUT") {
    // Track unique (propertyHash, label) pairs if label exists
    let pairId = property.propertyHash + "-" + property.label!
    let pair = PropertyLabelPair.load(pairId)
    
    // Only create if this pair doesn't exist yet (deduplication)
    if (!pair) {
      pair = new PropertyLabelPair(pairId)
      pair.propertyHash = property.propertyHash
      pair.label = property.label!
      pair.firstSeenTimestamp = event.block.timestamp
      pair.firstSeenBlockNumber = event.block.number
      pair.save()
      
      // Update counter for this label
      let counter = LabelCounter.load(property.label!)
      if (!counter) {
        counter = new LabelCounter(property.label!)
        counter.label = property.label!
        counter.uniquePropertyCount = 0
      }
      
      counter.uniquePropertyCount = counter.uniquePropertyCount + 1
      counter.save()
      
      log.info("New unique pair created: {} for label: {}, total count: {}", [
        pairId, 
        property.label!, 
        counter.uniquePropertyCount.toString()
      ])
    }
    
    // Track unique (propertyHash, submitter, label) combinations
    let submitterPairId = property.propertyHash + "-" + property.submitter + "-" + property.label!
    let submitterPair = PropertySubmitterPair.load(submitterPairId)
    
    // Only create if this submitter-property-label combination doesn't exist yet
    if (!submitterPair) {
      submitterPair = new PropertySubmitterPair(submitterPairId)
      submitterPair.propertyHash = property.propertyHash
      submitterPair.submitter = property.submitter
      submitterPair.label = property.label!
      submitterPair.firstSeenTimestamp = event.block.timestamp
      submitterPair.firstSeenBlockNumber = event.block.number
      submitterPair.save()
      
      // Update counter for this submitter-label combination
      let submitterCounterId = property.submitter + "-" + property.label!
      let submitterCounter = SubmitterLabelCounter.load(submitterCounterId)
      if (!submitterCounter) {
        submitterCounter = new SubmitterLabelCounter(submitterCounterId)
        submitterCounter.submitter = property.submitter
        submitterCounter.label = property.label!
        submitterCounter.uniquePropertyCount = 0
      }
      
      submitterCounter.uniquePropertyCount = submitterCounter.uniquePropertyCount + 1
      submitterCounter.save()
      
      log.info("New submitter-property pair: {} for submitter: {} label: {}, count: {}", [
        submitterPairId,
        property.submitter,
        property.label!,
        submitterCounter.uniquePropertyCount.toString()
      ])
    }
  }
}

export function handleDataGroupHeartBeat(event: DataGroupHeartBeat): void {
  // Create a unique ID using transaction hash and log index
  let id = event.transaction.hash.toHexString() + "-" + event.logIndex.toString()
  
  let property = new Property(id)
  
  // Set property fields from event parameters
  // DataGroupHeartBeat has: propertyHash, dataGroupHash, dataHash (indexed), submitter (not indexed)
  property.propertyHash = event.params.propertyHash.toHexString()
  property.dataGroupHash = event.params.dataGroupHash.toHexString()
  property.submitter = event.params.submitter.toHexString()
  property.dataHash = event.params.dataHash.toHexString()
  
  // Set owner as the submitter (based on your requirements)
  property.owner = event.params.submitter.toHexString()
  
  // Try to decode dataHash and fetch from IPFS
  // Convert bytes32 dataHash back to CID using CLI logic
  let cid = bytes32ToCID(event.params.dataHash as Bytes)
  
  // Debug logging
  log.info("Processing DataGroupHeartBeat - dataHash: {}, converted to CID: {}", [event.params.dataHash.toHexString(), cid])
  
  // Fetch content from IPFS with retry logic
  let ipfsResult = fetchFromIPFSWithRetry(cid, 3)
  
  if (ipfsResult) {
    log.info("✅ IPFS fetch successful for CID: {}, content length: {}", [cid, ipfsResult.length.toString()])
    property.dataContent = ipfsResult.toString()
    
    // Try to parse as JSON and extract label
    let jsonResult = json.try_fromBytes(ipfsResult)
    if (!jsonResult.isError) {
      let jsonObject = jsonResult.value.toObject()
      let labelValue = jsonObject.get("label")
      
      if (labelValue && !labelValue.isNull()) {
        property.label = labelValue.toString()
        log.info("✅ Successfully extracted label: '{}' for propertyHash: {} (HeartBeat)", [property.label!, property.propertyHash])
      } else {
        log.warning("⚠️ Label field not found in JSON for CID: {} (HeartBeat)", [cid])
        property.label = null
      }
    } else {
      log.error("❌ Failed to parse JSON from IPFS content for CID: {} (HeartBeat)", [cid])
      property.label = null
    }
  } else {
    // IPFS fetch failed - likely timeout
    log.error("❌ IPFS TIMEOUT: Failed to fetch CID: {} - Content exists but The Graph's IPFS node timed out (HeartBeat)", [cid])
    
    // Store the timeout info for debugging
    property.dataContent = "TIMEOUT:" + cid
    property.label = "TIMEOUT"
  }
  
  // Extract county from propertyHash IPFS chain  
  let county = extractCountyFromPropertyHash(property.propertyHash)
  if (county) {
    property.county = county
    log.info("✅ County extracted: '{}' for propertyHash: {} (HeartBeat)", [county, property.propertyHash])
  } else {
    property.county = null
    log.warning("⚠️ Failed to extract county for propertyHash: {} (HeartBeat)", [property.propertyHash])
  }
  
  // Set block and transaction information
  property.timestamp = event.block.timestamp
  property.blockNumber = event.block.number
  property.transactionHash = event.transaction.hash.toHexString()
  
  // Save the entity
  property.save()
  
  // Track county information for mining leaderboard (if county extracted successfully)
  if (property.county && property.county!.length > 0) {
    // Track unique (submitter, county) pairs for leaderboard
    let submitterCountyPairId = property.submitter + "-" + property.county!
    let submitterCountyPair = SubmitterCountyPair.load(submitterCountyPairId)
    
    if (!submitterCountyPair) {
      submitterCountyPair = new SubmitterCountyPair(submitterCountyPairId)
      submitterCountyPair.submitter = property.submitter
      submitterCountyPair.county = property.county!
      submitterCountyPair.firstSeenTimestamp = event.block.timestamp
      submitterCountyPair.firstSeenBlockNumber = event.block.number
      submitterCountyPair.save()
      
      log.info("New submitter-county pair from HeartBeat: {} mined county: {}", [property.submitter, property.county!])
    }
    
    // Track county-label combinations if label exists and is not TIMEOUT
    if (property.label && property.label!.length > 0 && property.label! != "TIMEOUT") {
      let submitterCountyLabelId = property.submitter + "-" + property.county! + "-" + property.label!
      let submitterCountyLabelPair = SubmitterCountyLabelPair.load(submitterCountyLabelId)
      
      if (!submitterCountyLabelPair) {
        submitterCountyLabelPair = new SubmitterCountyLabelPair(submitterCountyLabelId)
        submitterCountyLabelPair.submitter = property.submitter
        submitterCountyLabelPair.county = property.county!
        submitterCountyLabelPair.label = property.label!
        submitterCountyLabelPair.firstSeenTimestamp = event.block.timestamp
        submitterCountyLabelPair.firstSeenBlockNumber = event.block.number
        submitterCountyLabelPair.save()
        
        log.info("New submitter-county-label combination from HeartBeat: {} mined {} in county {}", [
          property.submitter, property.label!, property.county!
        ])
      }
      
      // Update counter for this specific submitter-county-label combination
      let submitterCountyLabelCounter = SubmitterCountyLabelCounter.load(submitterCountyLabelId)
      if (!submitterCountyLabelCounter) {
        submitterCountyLabelCounter = new SubmitterCountyLabelCounter(submitterCountyLabelId)
        submitterCountyLabelCounter.submitter = property.submitter
        submitterCountyLabelCounter.county = property.county!
        submitterCountyLabelCounter.label = property.label!
        submitterCountyLabelCounter.propertiesMined = 0
        submitterCountyLabelCounter.lastActivityTimestamp = event.block.timestamp
        submitterCountyLabelCounter.lastActivityBlockNumber = event.block.number
      }
      
      submitterCountyLabelCounter.propertiesMined = submitterCountyLabelCounter.propertiesMined + 1
      submitterCountyLabelCounter.lastActivityTimestamp = event.block.timestamp
      submitterCountyLabelCounter.lastActivityBlockNumber = event.block.number
      submitterCountyLabelCounter.save()
      
      log.info("Updated county-label stats from HeartBeat: {} has {} {} properties in {}", [
        property.submitter,
        submitterCountyLabelCounter.propertiesMined.toString(),
        property.label!,
        property.county!
      ])
    }
    
    // Update submitter's overall county counter (regardless of label)
    let submitterCounter = SubmitterCountyCounter.load(property.submitter)
    if (!submitterCounter) {
      submitterCounter = new SubmitterCountyCounter(property.submitter)
      submitterCounter.submitter = property.submitter
      submitterCounter.uniqueCounties = []
      submitterCounter.uniqueCountyCount = 0
      submitterCounter.totalPropertiesMined = 0
      submitterCounter.firstActivityTimestamp = event.block.timestamp
      submitterCounter.lastActivityTimestamp = event.block.timestamp
      submitterCounter.lastActivityBlockNumber = event.block.number
      submitterCounter.propertiesPerSecond = BigDecimal.fromString("0")
    }
    
    // Add county to list if not already present
    let counties = submitterCounter.uniqueCounties
    let countyExists = false
    for (let i = 0; i < counties.length; i++) {
      if (counties[i] == property.county!) {
        countyExists = true
        break
      }
    }
    
    if (!countyExists) {
      counties.push(property.county!)
      submitterCounter.uniqueCounties = counties
      submitterCounter.uniqueCountyCount = counties.length
    }
    
    submitterCounter.totalPropertiesMined = submitterCounter.totalPropertiesMined + 1
    submitterCounter.lastActivityTimestamp = event.block.timestamp
    submitterCounter.lastActivityBlockNumber = event.block.number
    
    // Calculate mining speed (properties per second)
    let timeElapsed = event.block.timestamp.minus(submitterCounter.firstActivityTimestamp)
    if (timeElapsed.gt(BigInt.fromI32(0))) {
      // timeElapsed is already in seconds, so we can directly calculate
      submitterCounter.propertiesPerSecond = BigInt.fromI32(submitterCounter.totalPropertiesMined).toBigDecimal().div(timeElapsed.toBigDecimal())
    }
    
    submitterCounter.save()
    
    log.info("Updated county stats: {} has {} unique counties, {} total properties, speed: {} props/sec", [
      property.submitter,
      submitterCounter.uniqueCountyCount.toString(),
      submitterCounter.totalPropertiesMined.toString(),
      submitterCounter.propertiesPerSecond.toString()
    ])
    
    // Update county-specific counter
    let countyCounter = CountyCounter.load(property.county!)
    if (!countyCounter) {
      countyCounter = new CountyCounter(property.county!)
      countyCounter.county = property.county!
      countyCounter.uniquePropertiesCount = 0
    }
    
    countyCounter.uniquePropertiesCount = countyCounter.uniquePropertiesCount + 1
    countyCounter.save()
    
    log.info("Mining stats updated from HeartBeat - submitter: {} county: {}", [
      property.submitter,
      property.county!
    ])
  }
  
  // Track unique (propertyHash, label) pairs if label exists and not TIMEOUT
  if (property.label && property.label!.length > 0 && property.label! != "TIMEOUT") {
    // Track unique (propertyHash, label) pairs if label exists
    let pairId = property.propertyHash + "-" + property.label!
    let pair = PropertyLabelPair.load(pairId)
    
    // Only create if this pair doesn't exist yet (deduplication)
    if (!pair) {
      pair = new PropertyLabelPair(pairId)
      pair.propertyHash = property.propertyHash
      pair.label = property.label!
      pair.firstSeenTimestamp = event.block.timestamp
      pair.firstSeenBlockNumber = event.block.number
      pair.save()
      
      // Update counter for this label
      let counter = LabelCounter.load(property.label!)
      if (!counter) {
        counter = new LabelCounter(property.label!)
        counter.label = property.label!
        counter.uniquePropertyCount = 0
      }
      
      counter.uniquePropertyCount = counter.uniquePropertyCount + 1
      counter.save()
      
      log.info("New unique pair created from HeartBeat: {} for label: {}, total count: {}", [
        pairId, 
        property.label!, 
        counter.uniquePropertyCount.toString()
      ])
    }
    
    // Track unique (propertyHash, submitter, label) combinations
    let submitterPairId = property.propertyHash + "-" + property.submitter + "-" + property.label!
    let submitterPair = PropertySubmitterPair.load(submitterPairId)
    
    // Only create if this submitter-property-label combination doesn't exist yet
    if (!submitterPair) {
      submitterPair = new PropertySubmitterPair(submitterPairId)
      submitterPair.propertyHash = property.propertyHash
      submitterPair.submitter = property.submitter
      submitterPair.label = property.label!
      submitterPair.firstSeenTimestamp = event.block.timestamp
      submitterPair.firstSeenBlockNumber = event.block.number
      submitterPair.save()
      
      // Update counter for this submitter-label combination
      let submitterCounterId = property.submitter + "-" + property.label!
      let submitterCounter = SubmitterLabelCounter.load(submitterCounterId)
      if (!submitterCounter) {
        submitterCounter = new SubmitterLabelCounter(submitterCounterId)
        submitterCounter.submitter = property.submitter
        submitterCounter.label = property.label!
        submitterCounter.uniquePropertyCount = 0
      }
      
      submitterCounter.uniquePropertyCount = submitterCounter.uniquePropertyCount + 1
      submitterCounter.save()
      
      log.info("New submitter-property pair from HeartBeat: {} for submitter: {} label: {}, count: {}", [
        submitterPairId,
        property.submitter,
        property.label!,
        submitterCounter.uniquePropertyCount.toString()
      ])
    }
  }
}