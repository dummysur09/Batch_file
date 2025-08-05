#!/bin/bash
# Master Parallel Downloader v3.3
# Disk usage throttling every 40 jobs at 75% storage limit
# GNU Parallel with enhanced progress bar and internet speed display
# Added JSON uploader file integration for skipping previously uploaded files

set -o pipefail
BASE_DIR="/home/co"
SOURCE_DIR="$BASE_DIR/1_source_scripts"
BATCH_DIR="$BASE_DIR/2_batches"
LOG_DIR="$BASE_DIR/3_logs"
DOWNLOAD_LOG="$LOG_DIR/download.log"
ERROR_LOG="$LOG_DIR/download_errors.log"
UPLOADER_JSON="$BASE_DIR/uploader.json"
JOB_LIST=$(mktemp)
PROGRESS_FILE=$(mktemp)
STATS_FILE=$(mktemp)
JSON_SKIPPED_COUNT=0

mkdir -p "$SOURCE_DIR" "$BATCH_DIR" "$LOG_DIR"
touch "$DOWNLOAD_LOG" "$ERROR_LOG" "$PROGRESS_FILE" "$STATS_FILE"

trap 'rm -f "$JOB_LIST" "$PROGRESS_FILE" "$STATS_FILE"' EXIT

echo "0" > "$PROGRESS_FILE"
echo "0 0 0" > "$STATS_FILE"  # completed failed total_size

# --- Color codes ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'
BOLD='\033[1m'

# --- Progress bar functions ---
format_bytes() {
    local bytes=$1
    if (( bytes < 1024 )); then
        echo "${bytes}B"
    elif (( bytes < 1048576 )); then
        echo "$(( bytes / 1024 ))KB"
    elif (( bytes < 1073741824 )); then
        echo "$(( bytes / 1048576 ))MB"
    else
        echo "$(( bytes / 1073741824 ))GB"
    fi
}

format_time() {
    local seconds=$1
    if (( seconds < 60 )); then
        echo "${seconds}s"
    elif (( seconds < 3600 )); then
        echo "$((seconds/60))m $((seconds%60))s"
    else
        echo "$((seconds/3600))h $((seconds%3600/60))m"
    fi
}

update_progress_bar() {
    local current=$1
    local total=$2
    local completed=$3
    local failed=$4
    local total_size=$5
    local start_time=$6

    if (( total == 0 )); then return; fi

    local percent=$(( current * 100 / total ))
    local elapsed=$(( $(date +%s) - start_time ))
    local rate=0
    local eta="∞"

    if (( elapsed > 0 && current > 0 )); then
        rate=$(( current / elapsed ))
        if (( rate > 0 )); then
            local remaining=$(( (total - current) / rate ))
            eta=$(format_time $remaining)
        fi
    fi

    local bar_width=50
    local filled=$(( percent * bar_width / 100 ))
    local bar=""
    for ((i=0; i<filled; i++)); do bar+="█"; done
    for ((i=filled; i<bar_width; i++)); do bar+="░"; done

    local status_color=$BLUE
    local status_icon="⬇️"
    if (( current == total )); then status_color=$GREEN; status_icon="✅"
    elif (( failed > 0 )); then status_color=$YELLOW; status_icon="⚠️"; fi

    local speed="0 files/s"
    if (( elapsed > 0 )); then
        local fps=$(( current * 10 / elapsed ))
        speed="$((fps/10)).$((fps%10)) files/s"
    fi

    printf "\r\033[K"
    printf "${status_color}${status_icon} ${BOLD}[%s]${NC} ${WHITE}%3d%%${NC} " "$bar" "$percent"
    printf "${CYAN}%d${NC}/${CYAN}%d${NC} " "$current" "$total"
    printf "${GREEN}✓%d${NC} " "$completed"
    if (( failed > 0 )); then
        printf "${RED}✗%d${NC} " "$failed"
    fi
    printf "${PURPLE}%s${NC} " "$speed"
    printf "${YELLOW}ETA:%s${NC} " "$eta"
    if (( total_size > 0 )); then
        printf "${BLUE}%s${NC}" "$(format_bytes $total_size)"
    fi
}

show_progress_monitor() {
    local total=$1
    local start_time=$(date +%s)
    while true; do
        if [[ ! -f "$PROGRESS_FILE" ]]; then break; fi
        local current=$(cat "$PROGRESS_FILE" 2>/dev/null || echo "0")
        local stats=($(cat "$STATS_FILE" 2>/dev/null || echo "0 0 0"))
        local completed=${stats[0]}
        local failed=${stats[1]}
        local total_size=${stats[2]}
        update_progress_bar "$current" "$total" "$completed" "$failed" "$total_size" "$start_time"
        if (( current >= total )); then echo; break; fi
        sleep 1
    done
}

log() {
    echo -e "$(date +'%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_DIR/master_downloader.log"
}

# Function to check if a file is already in the uploader.json
is_in_uploader_json() {
    local rel_path=$1
    
    # Check if uploader.json exists
    if [[ ! -f "$UPLOADER_JSON" ]]; then
        return 1  # File not found, so not in JSON
    fi
    
    # Use grep to check if the file_path exists in the JSON
    if grep -q "\"file_path\": \"$rel_path\"" "$UPLOADER_JSON"; then
        return 0  # Found in JSON
    else
        return 1  # Not found in JSON
    fi
}

parse_source_script() {
    local script_path="$1"
    log "🔎 Parsing source script: $(basename "$script_path")"
    local batch_name
    if grep -q '^batch=' "$script_path"; then
        batch_name=$(awk -F'=' '/^batch=/ {gsub(/["'\''"]/, "", $2); print $2}' "$script_path" | head -1)
    else
        log "⚠️ WARNING: Could not find batch name in '$script_path'. Using filename as fallback."
        batch_name=$(basename "$script_path" .txt)
    fi
    local download_base_dir="$BATCH_DIR/$batch_name"
    local current_relative_dir=""
    local dir_stack=()
    while IFS= read -r line; do
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        if [[ $line =~ mkdir[[:space:]]+\-p[[:space:]]+\"([^\"]+)\"[[:space:]]+\&\&[[:space:]]+cd[[:space:]]+\"([^\"]+)\" ]]; then
            local dir="${BASH_REMATCH[1]}"
            dir=$(echo "$dir" | sed "s|\$batch|$batch_name|g")
            current_relative_dir="$current_relative_dir/$dir"
            dir_stack+=("$dir")
        elif [[ $line =~ ^[[:space:]]*cd[[:space:]]+\"([^\"]+)\" ]] || [[ $line =~ ^[[:space:]]*cd[[:space:]]+([^[:space:]\"]+) ]]; then
            local target_dir="${BASH_REMATCH[1]}"
            [[ "$target_dir" =~ ^(/|~|\$HOME) ]] && continue
            target_dir=$(echo "$target_dir" | sed "s|\$batch|$batch_name|g")
            if [[ "$target_dir" == ".." ]]; then
                [[ ${#dir_stack[@]} -gt 0 ]] && unset "dir_stack[-1]"
                current_relative_dir=$(printf "/%s" "${dir_stack[@]}")
                [[ -z "$current_relative_dir" ]] && current_relative_dir=""
            else
                current_relative_dir="$current_relative_dir/$target_dir"
                dir_stack+=("$target_dir")
            fi
        fi
        if [[ $line =~ yt-dlp.*-o[[:space:]]+\"([^\"]+)\".*\"([^\"]+)\"[[:space:]]*$ ]]; then
            local filename="${BASH_REMATCH[1]}"
            local url="${BASH_REMATCH[2]}"
            
            # Remove potential duplicate path components
            local basename=$(basename "$filename")
            local dirname=$(dirname "$filename")
            local parent_dir=$(basename "$dirname")
            
            local final_path
            if [[ "$parent_dir" == "$basename" ]]; then
                # If the parent directory has the same name as the file, use the parent directory
                final_path="$download_base_dir$current_relative_dir/$dirname"
            else
                final_path="$download_base_dir$current_relative_dir/$filename"
            fi
            
            # Create relative path for JSON comparison
            local rel_path="${final_path#$BATCH_DIR/}"
            # First check if file is in uploader.json
            if is_in_uploader_json "$rel_path"; then
                log "🔍 Skipping file already in uploader.json: $rel_path"
                increment_progress
                ((JSON_SKIPPED_COUNT++))
            # Then check if it's already download
            
            elif [[ -f "$final_path" && -s "$final_path" ]] && grep -qF "$final_path" "$DOWNLOAD_LOG"; then
                log "🔍 Skipping already downloaded file: $final_path"
                increment_progress
            else
                echo "$url|$final_path" >> "$JOB_LIST"
            fi
        fi
    done < "$script_path"
    log "📁 Parsed batch: $batch_name"
}

check_disk_usage() {
    df --output=pcent "$BASE_DIR" 2>/dev/null | tail -1 | tr -d ' %'
}

increment_progress() {
    local current=$(cat "$PROGRESS_FILE")
    echo $((current + 1)) > "$PROGRESS_FILE"
}

update_stats() {
    local completed=$1
    local failed=$2
    local size=$3
    local current_stats=($(cat "$STATS_FILE"))
    echo "$((current_stats[0]+completed)) $((current_stats[1]+failed)) $((current_stats[2]+size))" > "$STATS_FILE"
}

download_file() {
    local url=$1
    local final_path=$2
    
    # Create parent directory
    mkdir -p "$(dirname "$final_path")"
    
    if yt-dlp --concurrent-fragments 32 --no-warnings --retries 3 --console-title --progress -o "$final_path" "$url" 2>/dev/null ; then
        if [[ -f "$final_path" && -s "$final_path" ]]; then
            echo "$final_path" >> "$DOWNLOAD_LOG"
            local size=$(stat -f%z "$final_path" 2>/dev/null || stat -c%s "$final_path" 2>/dev/null || echo 0)
            update_stats 1 0 "$size"
        else
            echo "$(date +"%Y-%m-%d %H:%M:%S") - FAILED - Empty/Missing file - URL: $url - Path: $final_path" >> "$ERROR_LOG"
            update_stats 0 1 0
        fi
    else
        echo "$(date +"%Y-%m-%d %H:%M:%S") - FAILED - Download error - URL: $url - Path: $final_path" >> "$ERROR_LOG"
        update_stats 0 1 0
    fi
    increment_progress
}

export -f download_file increment_progress update_stats
export BASE_DIR DOWNLOAD_LOG ERROR_LOG PROGRESS_FILE STATS_FILE

submit_jobs_with_disk_check() {
    local count=0
    while IFS="|" read -r url final_path; do
        ((count++))
        if (( count % 40 == 0 )); then
            while true; do
                local usage=$(check_disk_usage)
                if (( usage >= 75 )); then
                    echo -e "\n🚧 Disk usage at ${usage}%, pausing new jobs for 60s..." >&2
                    sleep 60
                else
                    break
                fi
            done
        fi
        if command -v parallel >/dev/null 2>&1; then
            echo "$url|$final_path"
        else
            download_file "$url" "$final_path"
        fi
    done < "$JOB_LIST"
}

log "${BOLD}🚀 Starting Master Downloader v3.3${NC}"

if [ -z "$(ls -A "$SOURCE_DIR" 2>/dev/null)" ]; then
    log "${RED}❌ ERROR: The source directory '$SOURCE_DIR' is empty. Please add batch scripts.${NC}"
    exit 1
fi

# Check if uploader.json exists
if [[ -f "$UPLOADER_JSON" ]]; then
    log "${BLUE}📋 Found uploader.json - will check for previously uploaded files${NC}"
else
    log "${YELLOW}⚠️ uploader.json not found at $UPLOADER_JSON - will only use download log for skipping${NC}"
fi

for script in "$SOURCE_DIR"/*; do
    [[ -f "$script" ]] && parse_source_script "$script"
done

JOB_COUNT=$(wc -l < "$JOB_LIST" 2>/dev/null || echo "0")
if (( JOB_COUNT > 0 )); then
    log "${GREEN}⚙️  Found $JOB_COUNT new files to download.${NC}"
    if (( JSON_SKIPPED_COUNT > 0 )); then
        log "${CYAN}🔍 Skipped $JSON_SKIPPED_COUNT files already in uploader.json${NC}"
    fi
    
    CPU_CORES=$(nproc)
    PARALLEL_JOBS=$(( CPU_CORES * 3 ))
    (( PARALLEL_JOBS == 0 )) && PARALLEL_JOBS=1

    log "${BLUE}🏎️  Starting parallel download with $PARALLEL_JOBS workers.${NC}"
    echo

    if command -v parallel >/dev/null 2>&1; then
        show_progress_monitor "$JOB_COUNT" &
        progress_pid=$!
        submit_jobs_with_disk_check | parallel --colsep "\\|" -j "$PARALLEL_JOBS" --no-notice download_file {1} {2}
        wait $progress_pid 2>/dev/null
    else
        log "${YELLOW}⚠️  GNU Parallel not found, running sequentially...${NC}"
        show_progress_monitor "$JOB_COUNT" &
        progress_pid=$!
        submit_jobs_with_disk_check
        wait $progress_ pid 2>/dev/null
    fi

    stats=($(cat "$STATS_FILE"))
    completed=${stats[0]} failed=${stats[1]} total_size=${stats[2]}

    echo
    log "${GREEN}📊 Download Summary:${NC}"
    log "   ${GREEN}✅ Completed: $completed files${NC}"
    (( failed > 0 )) && log "   ${RED}❌ Failed: $failed files${NC}"
    log "   ${BLUE}📁 Total size: $(format_bytes $total_size)${NC}"
else
    log "${GREEN}👍 All files from source scripts are already downloaded. Nothing to do.${NC}"
fi

log "${BOLD}🎉 Master Downloader finished.${NC}"
