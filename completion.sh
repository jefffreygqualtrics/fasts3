#!/bin/zsh
#_fasts3_complete() {
#    local s3Prefix=$(${COMP_LINE} | grep -oh "s3://.*")
#
#    COMPREPLY=()
#    local completions="1,2,3"
#    COMPREPLY=( $(compgen"1,2,3" )
#    return 0
#}
#
#complete -f -F _fasts3_complete fasts3
_foo()
{
    ORIG_COMP_WORDBREAKS="${COMP_WORDBREAKS}"
    COMP_WORDBREAKS=" "
    local cur
    _get_comp_words_by_ref -n : cur
    next=$(fasts3 ls $cur | awk '{print $2}' | sed 's/s3://g')
    COMPREPLY=($next)
    COMP_WORDBREAKS=${ORIG_COMP_WORDBREAKS}
}
complete -o nospace -F _foo fasts3
