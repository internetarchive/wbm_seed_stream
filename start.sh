

API_ONLY=false
COLLECTORS_ONLY=false
COLLECTORS_ARGS=""

while [[ $
    case $1 in
        --api-only)
            API_ONLY=true
            shift
            ;;
        --collectors-only)
            COLLECTORS_ONLY=true
            shift
            ;;
        --collectors)
            shift
            while [[ $
                COLLECTORS_ARGS="$COLLECTORS_ARGS $1"
                shift
            done
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--api-only] [--collectors-only] [--collectors collector1.py collector2.py ...]"
            exit 1
            ;;
    esac
done

if [[ "$API_ONLY" == true && "$COLLECTORS_ONLY" == true ]]; then
    echo "Error: Cannot use both --api-only and --collectors-only flags"
    exit 1
fi

if [[ "$API_ONLY" == true ]]; then
    uvicorn main:app --reload
elif [[ "$COLLECTORS_ONLY" == true ]]; then
    cd collectors
    if [[ -n "$COLLECTORS_ARGS" ]]; then
        python manager.py --collectors$COLLECTORS_ARGS
    else
        python manager.py
    fi
    cd ..
else
    uvicorn main:app --reload &
    API_PID=$!
    
    cd collectors
    if [[ -n "$COLLECTORS_ARGS" ]]; then
        python manager.py --collectors$COLLECTORS_ARGS &
    else
        python manager.py &
    fi
    MANAGER_PID=$!
    cd ..
    
    trap 'kill $API_PID $MANAGER_PID 2>/dev/null; exit' INT TERM
    
    wait
fi