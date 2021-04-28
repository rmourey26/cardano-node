global_cachedir_def="$HOME"/.cache/cardano-workbench
global_port_base_def=30000
global_runsdir_def=${global_runsdir:-$(realpath "$global_basedir/../../run")}
global_runsdir=$global_runsdir_def

usage_run() {
     usage "run" "Managing cluster runs" <<EOF

    print-env-config [ENV-CONFIG-OPTS..] ENV-CONFIG-FILE
                          Output an environment config file with following options:
        --cachedir DIR       Set the cache directory;  Defaults to $global_cachedir_def
        --port-base PORTNO   Set base port number
        --stagger-ports      Whether to allocate different ports for each node;
                               Default is no port staggering.

    list                  List cluster runs

    allocate BATCH-NAME PROFILE-NAME
                          Allocate a cluster run with the specified:
                            - batch key (no semantics attached)
                            - profile name
                          A unique name would be allocated for this run,
                            and a run alias 'current' will be created for it

    start NAME            Start the named run

  Options:

    --runsdir DIR         Set the runs directory.  Defaults to $global_runsdir_def
EOF
}

run() {
while test $# -gt 0
do case "$1" in
       --runsdir )
           global_runsdir=$2; shift;;
       * ) break;; esac; shift; done

local op=${1:-list}; test $# -gt 0 && shift

case "${op}" in
    print-env-config )
        local usage="USAGE: wb run OPTS.. print-env-config"

        local v=(
            --arg     cachedir      "$global_cachedir_def"
            --argjson port_base     "$global_port_base_def"
            --argjson stagger_ports 'false'
            )

        while test $# -gt 0; do case "$1" in
           --cachedir )      v=(--arg     cachedir      "$2"  "${v[@]}"); shift;;
           --port-base )     v=(--argjson port_base      $2   "${v[@]}"); shift;;
           --stagger-ports ) v=(--argjson stagger_ports  true "${v[@]}");;
           * ) fatal "wb run print-env-config: unknown args: $*";; esac
           shift; done

        jq '{ cachedir:      $cachedir
            , port_base;     $port_base
            , stagger_ports: $stagger_ports
            }' --null-input "${v[@]}";;
    list )
        test -d "$global_runsdir" && cd "$global_runsdir" &&
            ls | {
                ## Filter out aliases:
                grep -v 'current' || true; }
        ;;

    show | s )
        local usage="USAGE: wb run show RUN-NAME"
        local name=${1:?$usage}

        local dir=$global_runsdir/$name
        jq '.' "$dir"/meta.json
        ;;

    set-current )
        local usage="USAGE: wb run set-current RUN-NAME"
        local name=${1:?$usage}
        local dir=$global_runsdir/$name

        local sanity_tests=(
            -f "$dir"/profile.json -a
            -f "$dir"/meta.json
        )
        if ! test "${sanity_tests[@]}"
        then fatal "run fails sanity checks:  $name at $dir"; fi

        rm -f       "$global_runsdir"/current
        ln -s $name "$global_runsdir"/current

        msg "current run is:  $name at:  $dir"
        ;;

    current-run-path | current-path | path )
        realpath "$global_runsdir"/current;;

    current-run-name | current-name | name | current )
        basename "$(run current-path)";;

    current-run-meta | current-meta | meta )
        jq '.' "$(run current-path)"/meta.json;;

    current-run-profile | current-profile | profile | p )
        jq '.' "$(run current-path)"/profile.json;;

    allocate )
        local usage="USAGE: wb run allocate BATCH-NAME PROFILE-NAME"
        local batch=${1:?$usage}
        local prof=${2:?$usage}

        local epoch=$(date +'%s' --utc)
        local time=$(date +'%Y'-'%m'-'%d'-'%H.%M' --date=@$epoch --utc)
        local name=$time.$batch.$prof

        local dir=$global_runsdir/$name
        local failure_tests=(
            "$(realpath --canonicalize-missing "$dir")" = "$(realpath "$global_runsdir")"
            -o -d "$dir"
            -o -f "$dir"/meta.json
            -o -f "$dir"/profile.json
        )
        if test "${failure_tests[@]}"
        then fatal "bad, bad run name/run dir:  $name @ $dir"; fi

        if ! profile has-profile          "$prof"
        then fatal      "no such profile:  $prof"; fi

        mkdir -p "$global_cachedir" && test -w "$global_cachedir" ||
            fatal "failed to create writable cache directory:  $global_cachedir"

        mkdir -p "$dir" && test -w "$dir" ||
            fatal "failed to create writable run directory:  $dir"

        profile get "$prof" > "$dir"/profile.json
        profile node-specs    "$dir"/profile.json

        local args=(
            --arg name      $name
            --arg batch     $batch
            --arg prof      $prof
            --arg epoch     $epoch
            --arg time      $time

            ## Administrativia
            --arg cache_dir "$(realpath "$global_cachedir")"
            --arg port_base $port_base
        )
        jq_fmutate "$dir"/meta.json '. *
           { name:      $name
           , batch:     $batch
           , profile:   $prof
           , epoch:     $epoch
           , time:      $time

           ## Administrativia:
           , admin:
             { cache_dir:     $cache_dir
             , port_base:     $port_base
             , stagger_ports: $stagger_ports
             }
           }
           ' "${args[@]}"

        topology make    "$dir"/profile.json "$dir"/topology

        profile describe "$dir"/profile.json

        msg "allocated new run:  $name at $dir"
        ;;

    start )
        local usage="USAGE: wb run start RUN-NAME"
        local name=${1:-$()?$usage}

        run set-current "$name"
        local current_run_path=$(run current-path)
        local cache_dir=$(jq .cache_dir "$current_run_path"/meta.json)

        local genesis_args+=(
            ## Positionals:
            "$cache_dir"/genesis
            "$current_run_path"/profile.json
            "$current_run_path"/topology
            "$current_run_path"/genesis
        )
        genesis prepare "''${genesis_args[@]}"
        ;;

    ### Undocumented
    describe | d )
        cat <<EOF
global_runsdir=$global_runsdir
EOF
        ;;

    * ) usage_run;; esac
}
