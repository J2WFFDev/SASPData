# SASP Competition Analytics Requirements

## Data Aggregation Hierarchy

### 🎯 **String Level** (Raw Performance)
- **Input**: Individual string times (raw, total, penalty)
- **Processing**: Keep 4 fastest times per stage, drop slowest
- **Output**: Stage-level aggregated times

### 🎯 **Stage Level** (4 strings → 1 stage)
- **Input**: 4 fastest string times from string level
- **Processing**: Sum the 4 times
- **Output**: Single stage time per athlete

### 🎯 **Match Level** (4 stages → 1 match)
- **Input**: Stage times from all 4 stages
- **Processing**: Sum all 4 stage times
- **Output**: Complete match time per athlete
- **Validation**: Must have non-null, > 0 times that are not DNF/DQ

### 🎯 **Squad Level** (4 athletes → 1 squad)
- **Input**: Complete match times from 4 athletes in same squad
- **Processing**: Sum all 4 athlete match times
- **Output**: Squad total time
- **Rules**:
  - Squad must have 4 athletes from same division
  - Mixed divisions = "Open" classification
  - Incomplete Rookie squads: up to 2 "Ghost" athletes with 100-second default

## Competition Classifications

### 📊 **Divisions** (Age-based)
1. **Rookie**
2. **Intermediate** 
3. **Senior**
4. **Collegiate**

### 📊 **Classes** (Skill-based)
1. **Rookie**
2. **Intermediate/Entry**
3. **Intermediate/Advanced**
4. **Senior/JV**
5. **Senior/Varsity**
6. **Collegiate**

### 📊 **Demographics**
- **Gender**: Men, Women
- **Disciplines**: Iron Rifle, Optic Pistol, 1911, etc.

## Ranking Categories

### 🥇 **Individual Rankings (HOA - High Overall Aggregate)**
- **Scope**: Single athlete performance
- **Grouping**: By class + gender + discipline
- **Metric**: Lowest complete match time
- **Examples**:
  - "HOA 1st Place, Rookie class, Men, Iron Rifle"
  - "HOA 2nd Place, Intermediate/Advanced class, Women, Optic Pistol"

### 🥇 **Squad Rankings**
- **Scope**: 4-person team performance
- **Grouping**: By division + discipline
- **Metric**: Fastest complete squad time
- **Examples**:
  - "Squad 4th Place, Senior division, 1911 discipline"

## Data Quality Rules

### ✅ **Complete Match Criteria**
- All 4 stages must have times
- Times must be > 0
- Times must not be DNF (Did Not Finish)
- Times must not be DQ (Disqualified)

### ✅ **Complete Squad Criteria**
- Must have 4 athletes
- All 4 athletes must have complete matches
- Athletes must be from same division (or classified as "Open")
- Rookie exception: Up to 2 "Ghost" athletes allowed

### ✅ **String Processing Rules**
- 5 strings shot per stage
- Keep 4 fastest times
- Drop 1 slowest time
- Consider: raw time, total time, penalty time

## Reporting Perspectives

### 📈 **Detailed Analysis Views**
Users want to see performance data sliced by:
- **Athlete**: Individual performance history
- **Team**: Team member performances
- **Discipline**: Weapon/shooting type comparison
- **Gender**: Men vs Women analysis
- **Stage**: Stage-by-stage breakdown
- **Match**: Complete match analysis
- **Squad**: Team dynamics and cooperation

### 📈 **Combination Views**
- Athlete + Discipline + Gender (e.g., "Top women in Iron Rifle")
- Team + Match + Stage (e.g., "Team X stage 2 performance in Match Y")
- Division + Class + Discipline (e.g., "Senior/Varsity in 1911")

## Performance Metrics

### ⏱️ **Time-based Metrics**
- **String Time**: Individual string performance
- **Stage Time**: Sum of 4 best strings
- **Match Time**: Sum of 4 stages
- **Squad Time**: Sum of 4 athletes

### 📊 **Ranking Metrics**
- **Place**: 1st, 2nd, 3rd, etc. within category
- **Category**: Class + Gender + Discipline combination
- **Percentile**: Performance relative to peer group
- **Improvement**: Change over time/competitions

## Data Validation Requirements

### 🔍 **Completeness Checks**
- Verify 4 stages per match
- Verify 4-5 strings per stage
- Verify squad member count
- Verify division/class alignment

### 🔍 **Quality Checks**
- Flag unrealistic times (too fast/slow)
- Identify DNF/DQ entries
- Validate ghost athlete usage
- Check division consistency within squads

## Web Reporting Tool Objectives

### 🎯 **Primary Goals**
1. **Rankings Dashboard**: Live leaderboards by various categories
2. **Performance Analytics**: Trend analysis and improvement tracking
3. **Competition Management**: Real-time results and standings
4. **Historical Analysis**: Season-over-season comparisons
5. **Team Management**: Squad formation and performance tracking

### 🎯 **User Personas**
- **Athletes**: Personal performance tracking
- **Coaches**: Team and individual analysis
- **Match Directors**: Competition management
- **Parents**: Student progress monitoring
- **Officials**: Results verification and rankings