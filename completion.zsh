#!/bin/zsh
_fasts3_complete() {
    local s3Prefix completions
    s3Prefix="$1"
    if  [[ "$s3Prefix" =~ ^s3:// ]]; then
        completions="$(fasts3 ls $s3Prefix | awk '{print $2}')"
        reply=("${(ps:\n:)completions}")
    fi
}

compctl -K _fasts3_complete -S '' fasts3
