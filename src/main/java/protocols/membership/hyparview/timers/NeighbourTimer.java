package protocols.membership.hyparview.timers;

import babel.generic.ProtoTimer;

public class NeighbourTimer extends ProtoTimer{

    public static final short TIMER_ID = 6913;

    public NeighbourTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}
