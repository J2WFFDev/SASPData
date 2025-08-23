#!/usr/bin/env python3
"""
ETL Rankings Pipeline
Purpose: Populate rankings_individual and rankings_squad tables from performance data
Usage: python etl_rankings.py [--competition-key 1]
"""

import sys
import os
import logging
from typing import Optional
import argparse
from decimal import Decimal
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging():
    """Configure logging for ETL process"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

class RankingsETL:
    """ETL pipeline for calculating and loading rankings"""
    
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger(__name__)
    
    def clear_rankings(self, competition_key: Optional[int] = None):
        """Clear existing rankings data"""
        if competition_key:
            self.logger.info(f"Clearing rankings for competition {competition_key}")
            self.cursor.execute(
                "DELETE FROM rankings_individual WHERE competition_key = %s",
                (competition_key,)
            )
            self.cursor.execute(
                "DELETE FROM rankings_squad WHERE competition_key = %s", 
                (competition_key,)
            )
        else:
            self.logger.info("Clearing all rankings data")
            self.cursor.execute("TRUNCATE TABLE rankings_individual CASCADE")
            self.cursor.execute("TRUNCATE TABLE rankings_squad CASCADE")
    
    def calculate_individual_rankings(self, competition_key: Optional[int] = None):
        """Calculate individual athlete rankings by class+gender+discipline"""
        self.logger.info("Calculating individual rankings...")
        
        # Build WHERE clause for competition filter
        where_clause = ""
        params = []
        if competition_key:
            where_clause = "WHERE sp.competition_key = %s"
            params = [competition_key]
        
        sql = f"""
        WITH athlete_performance AS (
            -- Get best stage performance per athlete per discipline
            SELECT 
                sp.competition_key,
                sp.athlete_key,
                sp.discipline_key,
                a.gender,
                COALESCE(dc.division_name, 'Open') as classification,
                MIN(sp.total_time) as best_time,
                AVG(sp.total_time) as avg_time,
                COUNT(*) as stages_completed
            FROM fact_stage_performance sp
            JOIN dim_athlete a ON sp.athlete_key = a.athlete_key
            LEFT JOIN dim_classification dc ON a.classification_key = dc.classification_key
            {where_clause}
            GROUP BY sp.competition_key, sp.athlete_key, sp.discipline_key, 
                     a.gender, dc.division_name
        ),
        rankings AS (
            -- Calculate rankings within class+gender+discipline
            SELECT 
                ap.*,
                DENSE_RANK() OVER (
                    PARTITION BY ap.competition_key, ap.classification, ap.gender, ap.discipline_key 
                    ORDER BY ap.best_time ASC
                ) as rank_position,
                COUNT(*) OVER (
                    PARTITION BY ap.competition_key, ap.classification, ap.gender, ap.discipline_key
                ) as total_athletes,
                PERCENT_RANK() OVER (
                    PARTITION BY ap.competition_key, ap.classification, ap.gender, ap.discipline_key 
                    ORDER BY ap.best_time ASC
                ) * 100 as percentile
            FROM athlete_performance ap
        )
        INSERT INTO rankings_individual (
            competition_key, athlete_key, discipline_key, classification_key,
            ranking_category, classification_name, gender, discipline_name,
            athlete_name, best_time, average_time, stages_completed,
            overall_rank, total_athletes, percentile, award_level, is_hoa_winner
        )
        SELECT 
            r.competition_key,
            r.athlete_key,
            r.discipline_key,
            a.classification_key,
            
            -- Category string
            CONCAT(r.classification, ' ', INITCAP(r.gender), ' ', d.name) as ranking_category,
            r.classification,
            r.gender,
            d.name as discipline_name,
            
            -- Athlete info
            CONCAT(a.fname, ' ', a.lname) as athlete_name,
            r.best_time,
            r.avg_time,
            r.stages_completed,
            
            -- Rankings
            r.rank_position as overall_rank,
            r.total_athletes,
            ROUND(r.percentile::numeric, 2) as percentile,
            
            -- Awards
            CASE 
                WHEN r.rank_position = 1 THEN '1st Place HOA'
                WHEN r.rank_position = 2 THEN '2nd Place HOA'
                WHEN r.rank_position = 3 THEN '3rd Place HOA'
                WHEN r.rank_position <= 5 THEN 'Top 5'
                WHEN r.percentile <= 10 THEN 'Top 10%'
                WHEN r.percentile <= 25 THEN 'Top 25%'
                ELSE NULL
            END as award_level,
            
            -- HOA Winner (1st place in category)
            (r.rank_position = 1) as is_hoa_winner
            
        FROM rankings r
        JOIN dim_athlete a ON r.athlete_key = a.athlete_key
        JOIN dim_discipline d ON r.discipline_key = d.discipline_key
        ORDER BY r.competition_key, r.classification, r.gender, r.discipline_key, r.rank_position
        """
        
        result = self.cursor.execute(sql, params)
        count = self.cursor.rowcount
        self.logger.info(f"Inserted {count} individual rankings")
        return count
    
    def calculate_squad_rankings(self, competition_key: Optional[int] = None):
        """Calculate squad rankings by division+discipline"""
        self.logger.info("Calculating squad rankings...")
        
        # Build WHERE clause for competition filter
        where_clause = ""
        params = []
        if competition_key:
            where_clause = "WHERE fsp.competition_key = %s"
            params = [competition_key]
        
        sql = f"""
        WITH squad_members AS (
            -- Get top 4 performers per team per discipline
            SELECT 
                fsp.competition_key,
                fsp.team_key,
                fsp.discipline_key,
                fsp.athlete_key,
                fsp.total_time,
                ROW_NUMBER() OVER (
                    PARTITION BY fsp.competition_key, fsp.team_key, fsp.discipline_key 
                    ORDER BY fsp.total_time ASC
                ) as member_rank
            FROM fact_squad_performance fsp
            {where_clause}
        ),
        squad_performance AS (
            -- Aggregate squad performance
            SELECT 
                sm.competition_key,
                sm.team_key,
                sm.discipline_key,
                SUM(sm.total_time) as squad_total_time,
                AVG(sm.total_time) as squad_avg_time,
                COUNT(*) as members_count,
                
                -- Get individual member times for reference
                MAX(CASE WHEN member_rank = 1 THEN sm.athlete_key END) as member1_athlete_key,
                MAX(CASE WHEN member_rank = 1 THEN sm.total_time END) as member1_time,
                MAX(CASE WHEN member_rank = 2 THEN sm.athlete_key END) as member2_athlete_key,
                MAX(CASE WHEN member_rank = 2 THEN sm.total_time END) as member2_time,
                MAX(CASE WHEN member_rank = 3 THEN sm.athlete_key END) as member3_athlete_key,
                MAX(CASE WHEN member_rank = 3 THEN sm.total_time END) as member3_time,
                MAX(CASE WHEN member_rank = 4 THEN sm.athlete_key END) as member4_athlete_key,
                MAX(CASE WHEN member_rank = 4 THEN sm.total_time END) as member4_time
                
            FROM squad_members sm
            WHERE sm.member_rank <= 4  -- Top 4 only
            GROUP BY sm.competition_key, sm.team_key, sm.discipline_key
        ),
        squad_rankings AS (
            -- Calculate rankings within division+discipline
            SELECT 
                sp.*,
                COALESCE(dc.division_name, 'Open') as division_name,
                DENSE_RANK() OVER (
                    PARTITION BY sp.competition_key, dc.classification_name, sp.discipline_key 
                    ORDER BY sp.squad_total_time ASC
                ) as rank_position,
                COUNT(*) OVER (
                    PARTITION BY sp.competition_key, dc.classification_name, sp.discipline_key
                ) as total_squads,
                PERCENT_RANK() OVER (
                    PARTITION BY sp.competition_key, dc.classification_name, sp.discipline_key 
                    ORDER BY sp.squad_total_time ASC
                ) * 100 as percentile
            FROM squad_performance sp
            JOIN dim_team t ON sp.team_key = t.team_key
            LEFT JOIN dim_classification dc ON t.classification_key = dc.classification_key
        )
        INSERT INTO rankings_squad (
            competition_key, team_key, discipline_key, classification_key,
            ranking_category, division_name, discipline_name, squad_name,
            members_count, total_time, average_time, overall_rank, total_squads,
            percentile, award_level, is_division_winner,
            is_complete_squad, member1_athlete_key, member1_time,
            member2_athlete_key, member2_time, member3_athlete_key, member3_time,
            member4_athlete_key, member4_time
        )
        SELECT 
            sr.competition_key,
            sr.team_key,
            sr.discipline_key,
            t.classification_key,
            
            -- Category string
            CONCAT(sr.division_name, ' Division ', d.name) as ranking_category,
            sr.division_name,
            d.name as discipline_name,
            CONCAT(t.name, ' - ', d.name) as squad_name,
            
            -- Performance
            sr.members_count,
            sr.squad_total_time as total_time,
            sr.squad_avg_time as average_time,
            
            -- Rankings
            sr.rank_position as overall_rank,
            sr.total_squads,
            ROUND(sr.percentile::numeric, 2) as percentile,
            
            -- Awards
            CASE 
                WHEN sr.rank_position = 1 THEN '1st Place'
                WHEN sr.rank_position = 2 THEN '2nd Place'
                WHEN sr.rank_position = 3 THEN '3rd Place'
                WHEN sr.rank_position <= 5 THEN 'Top 5'
                WHEN sr.percentile <= 10 THEN 'Top 10%'
                WHEN sr.percentile <= 25 THEN 'Top 25%'
                ELSE NULL
            END as award_level,
            
            -- Division Winner
            (sr.rank_position = 1) as is_division_winner,
            
            -- Squad composition
            (sr.members_count = 4) as is_complete_squad,
            
            -- Member details
            sr.member1_athlete_key, sr.member1_time,
            sr.member2_athlete_key, sr.member2_time,
            sr.member3_athlete_key, sr.member3_time,
            sr.member4_athlete_key, sr.member4_time
            
        FROM squad_rankings sr
        JOIN dim_team t ON sr.team_key = t.team_key
        JOIN dim_discipline d ON sr.discipline_key = d.discipline_key
        ORDER BY sr.competition_key, sr.division_name, sr.discipline_key, sr.rank_position
        """
        
        result = self.cursor.execute(sql, params)
        count = self.cursor.rowcount
        self.logger.info(f"Inserted {count} squad rankings")
        return count
    
    def run_full_rankings_etl(self, competition_key: Optional[int] = None):
        """Run complete rankings ETL pipeline"""
        self.logger.info("Starting rankings ETL pipeline")
        
        try:
            # Clear existing rankings
            self.clear_rankings(competition_key)
            
            # Calculate individual rankings
            individual_count = self.calculate_individual_rankings(competition_key)
            
            # Calculate squad rankings
            squad_count = self.calculate_squad_rankings(competition_key)
            
            self.logger.info(f"Rankings ETL completed successfully:")
            self.logger.info(f"  Individual rankings: {individual_count}")
            self.logger.info(f"  Squad rankings: {squad_count}")
            
            return individual_count, squad_count
            
        except Exception as e:
            self.logger.error(f"Rankings ETL failed: {e}")
            raise

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Generate competitive shooting rankings')
    parser.add_argument('--competition-key', type=int, 
                       help='Process rankings for specific competition only')
    parser.add_argument('--clear-only', action='store_true',
                       help='Only clear existing rankings, do not regenerate')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        
        # Initialize ETL
        etl = RankingsETL()
        
        if args.clear_only:
            logger.info("Clearing rankings only...")
            etl.clear_rankings(args.competition_key)
            logger.info("Rankings cleared successfully")
        else:
            # Run full ETL
            individual_count, squad_count = etl.run_full_rankings_etl(args.competition_key)
            
            print(f"\\nRankings ETL Results:")
            print(f"  Individual rankings: {individual_count:,}")
            print(f"  Squad rankings: {squad_count:,}")
            print(f"\\nETL completed successfully!")
            
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        print(f"\\nError: {e}")
        sys.exit(1)
    
    finally:
        if 'etl' in locals():
            etl.conn.close()

if __name__ == "__main__":
    main()