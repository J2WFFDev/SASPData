#!/usr/bin/env python3
"""
ETL Aggregation Pipeline for Performance Tables
Implements the core analytics rules:
1. Drop slowest string, keep 4 fastest per stage
2. Sum stage performance into match performance 
3. Sum match performance into squad performance
"""

import psycopg2
from dotenv import load_dotenv
import os
import sys
from decimal import Decimal
from typing import List, Dict, Tuple, Optional

# Load environment variables
load_dotenv()

class PerformanceETL:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
    
    def drop_slowest_string(self, strings: List[Tuple]) -> Tuple[List[Tuple], Optional[int]]:
        """
        Drop the slowest string from 5 strings, return 4 fastest + dropped string number.
        strings: List of (string_num, raw_time, total_time, penalties)
        Returns: (4_fastest_strings, dropped_string_number or None)
        """
        if len(strings) <= 4:
            return strings, None  # No dropping needed
        
        # Sort by total_time (raw + penalties), find slowest
        strings_with_idx = [(s[0], s[1], s[2], s[3], idx) for idx, s in enumerate(strings)]
        strings_sorted = sorted(strings_with_idx, key=lambda x: x[2] or 999.999)  # total_time
        
        # Drop the slowest (last in sorted list)
        dropped = strings_sorted[-1]
        fastest_4 = strings_sorted[:-1]
        
        return [(s[0], s[1], s[2], s[3]) for s in fastest_4], dropped[0]
    
    def aggregate_stage_performance(self, limit: int = None) -> int:
        """
        Populate fact_stage_performance from fact_entry_strings.
        Implements: Keep 4 fastest strings per stage, drop slowest.
        """
        print("🎯 Aggregating stage performance (drop slowest string)...")
        
        with self.conn.cursor() as cur:
            # Clear existing data
            cur.execute("TRUNCATE TABLE fact_stage_performance CASCADE;")
            
            # Get all entries with their strings grouped by stage
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            cur.execute(f"""
                SELECT 
                    fe.entry_id,
                    fe.competition_key,
                    fe.team_key, 
                    fe.athlete_key,
                    fe.discipline_key,
                    fe.slot_key,
                    fes.stage_no,
                    fes.string_no,
                    fes.time_value as raw_time,
                    fes.total_value as total_time,
                    fes.penalty_value as penalties
                FROM fact_entry fe
                JOIN fact_entry_strings fes ON fe.entry_id = fes.entry_id
                WHERE fes.time_value IS NOT NULL 
                ORDER BY fe.entry_id, fes.stage_no, fes.string_no
                {limit_clause};
            """)
            
            # Group by entry and stage
            entries_stages = {}
            for row in cur.fetchall():
                entry_id, comp_key, team_key, athlete_key, disc_key, slot_key, stage_no, string_no, raw_time, total_time, penalties = row
                
                key = (entry_id, comp_key, team_key, athlete_key, disc_key, slot_key, stage_no)
                if key not in entries_stages:
                    entries_stages[key] = []
                
                entries_stages[key].append((string_no, raw_time, total_time, penalties))
            
            print(f"📊 Processing {len(entries_stages):,} entry-stage combinations...")
            
            # Process each entry-stage combination
            stage_records = []
            for (entry_id, comp_key, team_key, athlete_key, disc_key, slot_key, stage_no), strings in entries_stages.items():
                
                # Get stage_key from dim_stage using stage number
                cur.execute("SELECT stage_key FROM dim_stage WHERE stage_number = %s;", (stage_no,))
                stage_key_result = cur.fetchone()
                if not stage_key_result:
                    print(f"⚠️  Warning: No stage_key found for stage number {stage_no}, skipping...")
                    continue
                
                stage_key = stage_key_result[0]
                
                # Drop slowest string
                fastest_4, dropped_string = self.drop_slowest_string(strings)
                
                if len(fastest_4) == 0:
                    continue
                
                # Calculate totals
                total_raw = sum(s[1] for s in fastest_4 if s[1] is not None)
                total_total = sum(s[2] for s in fastest_4 if s[2] is not None) 
                total_penalties = sum(s[3] for s in fastest_4 if s[3] is not None)
                
                # Pad strings to 4 if needed
                while len(fastest_4) < 4:
                    fastest_4.append((0, None, None, None))
                
                stage_records.append((
                    entry_id, stage_key, comp_key, team_key, athlete_key, disc_key, slot_key,
                    total_raw, total_total, total_penalties, len([s for s in fastest_4 if s[1] is not None]),
                    dropped_string,
                    fastest_4[0][1], fastest_4[0][2], fastest_4[0][3],  # string1
                    fastest_4[1][1], fastest_4[1][2], fastest_4[1][3],  # string2  
                    fastest_4[2][1], fastest_4[2][2], fastest_4[2][3],  # string3
                    fastest_4[3][1], fastest_4[3][2], fastest_4[3][3]   # string4
                ))
            
            # Bulk insert stage performance records
            if stage_records:
                cur.executemany("""
                    INSERT INTO fact_stage_performance (
                        entry_id, stage_key, competition_key, team_key, athlete_key, 
                        discipline_key, slot_key, total_raw_time, total_total_time, 
                        total_penalties, strings_count, dropped_string_number,
                        string1_raw, string1_total, string1_penalties,
                        string2_raw, string2_total, string2_penalties, 
                        string3_raw, string3_total, string3_penalties,
                        string4_raw, string4_total, string4_penalties
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, stage_records)
                
                self.conn.commit()
                print(f"✅ Inserted {len(stage_records):,} stage performance records")
                return len(stage_records)
            else:
                print("⚠️  No stage performance records to insert")
                return 0
    
    def aggregate_match_performance(self) -> int:
        """
        Populate fact_match_performance from fact_stage_performance.
        Implements: Sum all 4 stages into complete match times.
        """
        print("🎯 Aggregating match performance (sum 4 stages)...")
        
        with self.conn.cursor() as cur:
            # Clear existing data
            cur.execute("TRUNCATE TABLE fact_match_performance CASCADE;")
            
            # Aggregate stages into matches
            cur.execute("""
                INSERT INTO fact_match_performance (
                    entry_id, competition_key, team_key, athlete_key, 
                    discipline_key, slot_key, classification_key,
                    total_raw_time, total_total_time, total_penalties, stages_count,
                    stage1_raw, stage1_total, stage1_penalties,
                    stage2_raw, stage2_total, stage2_penalties,
                    stage3_raw, stage3_total, stage3_penalties, 
                    stage4_raw, stage4_total, stage4_penalties,
                    division_name, class_name
                )
                SELECT 
                    fsp.entry_id,
                    fsp.competition_key,
                    fsp.team_key,
                    fsp.athlete_key,
                    fsp.discipline_key,
                    fsp.slot_key,
                    da.classification_key,
                    
                    -- Match totals (sum of all stages)
                    SUM(fsp.total_raw_time) as total_raw_time,
                    SUM(fsp.total_total_time) as total_total_time,
                    SUM(fsp.total_penalties) as total_penalties,
                    COUNT(*) as stages_count,
                    
                    -- Individual stage totals
                    MAX(CASE WHEN ds.stage_number = 1 THEN fsp.total_raw_time END) as stage1_raw,
                    MAX(CASE WHEN ds.stage_number = 1 THEN fsp.total_total_time END) as stage1_total,
                    MAX(CASE WHEN ds.stage_number = 1 THEN fsp.total_penalties END) as stage1_penalties,
                    MAX(CASE WHEN ds.stage_number = 2 THEN fsp.total_raw_time END) as stage2_raw,
                    MAX(CASE WHEN ds.stage_number = 2 THEN fsp.total_total_time END) as stage2_total,
                    MAX(CASE WHEN ds.stage_number = 2 THEN fsp.total_penalties END) as stage2_penalties,
                    MAX(CASE WHEN ds.stage_number = 3 THEN fsp.total_raw_time END) as stage3_raw,
                    MAX(CASE WHEN ds.stage_number = 3 THEN fsp.total_total_time END) as stage3_total,
                    MAX(CASE WHEN ds.stage_number = 3 THEN fsp.total_penalties END) as stage3_penalties,
                    MAX(CASE WHEN ds.stage_number = 4 THEN fsp.total_raw_time END) as stage4_raw,
                    MAX(CASE WHEN ds.stage_number = 4 THEN fsp.total_total_time END) as stage4_total,
                    MAX(CASE WHEN ds.stage_number = 4 THEN fsp.total_penalties END) as stage4_penalties,
                    
                    -- Classification
                    da.division_name,
                    da.class_name
                    
                FROM fact_stage_performance fsp
                JOIN dim_stage ds ON fsp.stage_key = ds.stage_key
                LEFT JOIN dim_athlete da ON fsp.athlete_key = da.athlete_key
                GROUP BY 
                    fsp.entry_id, fsp.competition_key, fsp.team_key, fsp.athlete_key,
                    fsp.discipline_key, fsp.slot_key, da.classification_key,
                    da.division_name, da.class_name
                HAVING COUNT(*) >= 1;  -- At least 1 stage required
            """)
            
            match_count = cur.rowcount
            self.conn.commit()
            print(f"✅ Inserted {match_count:,} match performance records")
            return match_count

    def aggregate_squad_performance(self) -> int:
        """
        Aggregate match performance into squad performance.
        Implements: "total match data for all 4 members of the same squad for squad data"
        
        Returns:
            Number of squad records processed
        """
        print("🏆 Aggregating squad performance (sum 4 team members)...")
        
        with self.conn.cursor() as cur:
            # Clear existing squad performance data
            cur.execute("TRUNCATE TABLE fact_squad_performance")
            
            # Aggregate match data by squad (team + discipline + competition)
            cur.execute("""
                INSERT INTO fact_squad_performance (
                    competition_key,
                    team_key,
                    discipline_key,
                    classification_key,
                    squad_name,
                    
                    -- Squad totals (sum of all team members)
                    total_raw_time,
                    total_total_time,
                    total_penalties,
                    
                    -- Member tracking
                    members_count,
                    member1_entry_id,
                    member1_athlete_key,
                    member1_slot_key,
                    member1_raw,
                    member1_total,
                    member1_penalties,
                    member2_entry_id,
                    member2_athlete_key,
                    member2_slot_key,
                    member2_raw,
                    member2_total,
                    member2_penalties,
                    member3_entry_id,
                    member3_athlete_key,
                    member3_slot_key,
                    member3_raw,
                    member3_total,
                    member3_penalties,
                    member4_entry_id,
                    member4_athlete_key,
                    member4_slot_key,
                    member4_raw,
                    member4_total,
                    member4_penalties,
                    
                    -- Squad classification
                    division_name,
                    is_mixed_division,
                    has_ghost_athletes,
                    
                    created_at,
                    updated_at
                )
                SELECT 
                    fmp.competition_key,
                    fmp.team_key,
                    fmp.discipline_key,
                    
                    -- Use first member's classification as squad baseline
                    MAX(fmp.classification_key) as classification_key,
                    
                    -- Squad name
                    CONCAT(t.name, ' - ', d.name) as squad_name,
                    
                    -- Squad totals
                    SUM(fmp.total_raw_time) as total_raw_time,
                    SUM(fmp.total_total_time) as total_total_time,
                    SUM(fmp.total_penalties) as total_penalties,
                    
                    -- Member count
                    COUNT(*)::smallint as members_count,
                    
                    -- Individual member data (ordered by athlete_key for consistency)
                    MAX(CASE WHEN rn = 1 THEN fmp.entry_id END) as member1_entry_id,
                    MAX(CASE WHEN rn = 1 THEN fmp.athlete_key END) as member1_athlete_key,
                    MAX(CASE WHEN rn = 1 THEN fmp.slot_key END) as member1_slot_key,
                    MAX(CASE WHEN rn = 1 THEN fmp.total_raw_time END) as member1_raw,
                    MAX(CASE WHEN rn = 1 THEN fmp.total_total_time END) as member1_total,
                    MAX(CASE WHEN rn = 1 THEN fmp.total_penalties END) as member1_penalties,
                    MAX(CASE WHEN rn = 2 THEN fmp.entry_id END) as member2_entry_id,
                    MAX(CASE WHEN rn = 2 THEN fmp.athlete_key END) as member2_athlete_key,
                    MAX(CASE WHEN rn = 2 THEN fmp.slot_key END) as member2_slot_key,
                    MAX(CASE WHEN rn = 2 THEN fmp.total_raw_time END) as member2_raw,
                    MAX(CASE WHEN rn = 2 THEN fmp.total_total_time END) as member2_total,
                    MAX(CASE WHEN rn = 2 THEN fmp.total_penalties END) as member2_penalties,
                    MAX(CASE WHEN rn = 3 THEN fmp.entry_id END) as member3_entry_id,
                    MAX(CASE WHEN rn = 3 THEN fmp.athlete_key END) as member3_athlete_key,
                    MAX(CASE WHEN rn = 3 THEN fmp.slot_key END) as member3_slot_key,
                    MAX(CASE WHEN rn = 3 THEN fmp.total_raw_time END) as member3_raw,
                    MAX(CASE WHEN rn = 3 THEN fmp.total_total_time END) as member3_total,
                    MAX(CASE WHEN rn = 3 THEN fmp.total_penalties END) as member3_penalties,
                    MAX(CASE WHEN rn = 4 THEN fmp.entry_id END) as member4_entry_id,
                    MAX(CASE WHEN rn = 4 THEN fmp.athlete_key END) as member4_athlete_key,
                    MAX(CASE WHEN rn = 4 THEN fmp.slot_key END) as member4_slot_key,
                    MAX(CASE WHEN rn = 4 THEN fmp.total_raw_time END) as member4_raw,
                    MAX(CASE WHEN rn = 4 THEN fmp.total_total_time END) as member4_total,
                    MAX(CASE WHEN rn = 4 THEN fmp.total_penalties END) as member4_penalties,
                    
                    -- Squad classification logic
                    MAX(fmp.division_name) as division_name,
                    CASE 
                        WHEN COUNT(DISTINCT fmp.division_name) > 1 THEN true
                        ELSE false
                    END as is_mixed_division,
                    
                    -- Ghost athlete detection (for Rookie division)
                    CASE 
                        WHEN MAX(fmp.division_name) = 'Rookie' AND COUNT(*) < 4 THEN true
                        ELSE false
                    END as has_ghost_athletes,
                    
                    CURRENT_TIMESTAMP as created_at,
                    CURRENT_TIMESTAMP as updated_at
                    
                FROM (
                    SELECT fmp.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY fmp.competition_key, fmp.team_key, fmp.discipline_key 
                               ORDER BY fmp.total_total_time ASC  -- Select 4 best performers (lowest time)
                           ) as rn
                    FROM fact_match_performance fmp
                    WHERE fmp.total_total_time IS NOT NULL
                      AND fmp.total_total_time > 0
                ) fmp
                JOIN dim_team t ON fmp.team_key = t.team_key
                JOIN dim_discipline d ON fmp.discipline_key = d.discipline_key
                WHERE fmp.rn <= 4  -- Only take top 4 performers per squad
                GROUP BY 
                    fmp.competition_key, fmp.team_key, fmp.discipline_key,
                    t.name, d.name
                HAVING COUNT(*) >= 1;  -- At least 1 member required
            """)
            
            squad_count = cur.rowcount
            self.conn.commit()
            print(f"✅ Inserted {squad_count:,} squad performance records")
            return squad_count
    
    def run_full_pipeline(self, stage_limit: int = None) -> Dict[str, int]:
        """Run the complete aggregation pipeline."""
        print("🚀 Starting Performance ETL Aggregation Pipeline")
        print("=" * 60)
        
        results = {}
        
        try:
            # Stage 1: Aggregate stage performance (drop slowest)
            results['stage_records'] = self.aggregate_stage_performance(limit=stage_limit)
            
            # Stage 2: Aggregate match performance (sum stages)
            results['match_records'] = self.aggregate_match_performance()
            
            # Stage 3: Aggregate squad performance (sum team members)
            results['squad_records'] = self.aggregate_squad_performance()
            
            print("\n🎉 Pipeline completed successfully!")
            print(f"📊 Results Summary:")
            print(f"   • Stage performance records: {results['stage_records']:,}")
            print(f"   • Match performance records: {results['match_records']:,}")
            print(f"   • Squad performance records: {results['squad_records']:,}")
            
            return results
            
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            self.conn.rollback()
            raise
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

def main():
    """Main entry point."""
    # Parse command line arguments
    stage_limit = None
    if len(sys.argv) > 1:
        try:
            stage_limit = int(sys.argv[1])
            print(f"🔧 Running with stage limit: {stage_limit:,}")
        except ValueError:
            print("⚠️  Invalid limit argument, running without limit")
    
    etl = PerformanceETL()
    try:
        results = etl.run_full_pipeline(stage_limit=stage_limit)
        return results
    finally:
        etl.close()

if __name__ == "__main__":
    main()