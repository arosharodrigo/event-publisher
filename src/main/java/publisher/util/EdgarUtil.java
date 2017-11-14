package publisher.util;

public class EdgarUtil {

    public static int generateRandomTps() {
        return 730;
    }

    public static int generateTps(long count) {
        if (count > 2000 && count <= 27000) {
            return generateTpsFirstSpike(count-1000);
        } else if(count > 45000 && count <= 60000) {
            return generateTpsSecondSpike(count-45000);
        } else if(count > 77500 && count <= 110000) {
            return generateTpsLastSpike(count-77500);
        } else {
            return 225;
        }
    }

    public static int generateTpsFirstSpike(long count) {
        if (count > 1000 && count <= 2000) {
            return handle1000Up(count, 1000);
        } else if (count > 2000 && count <= 2200) {
            return 300;
        } else if (count > 2200 && count <= 2400) {
            return 500;
        } else if (count > 2400 && count <= 2500) {
            return 100;
        } else if (count > 2500 && count <= 3500) {
            return handle1000Up(count, 2500);
        } else if (count > 3500 && count <= 3700) {
            return 300;
        } else if (count > 3700 && count <= 4200) {
            return handle500Up(count, 3700);
        } else if (count > 4200 && count <= 4300) {
            return 100;
        } else if (count > 4300 && count <= 4500) {
            return 350;
        } else if (count > 4500 && count <= 5000) {
            return handle500Up(count, 4500);
        } else if (count > 5000 && count <= 5250) {
            return 100;
        } else if (count > 5250 && count <= 5750) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 5750 && count <= 6000) {
            return 300;
        } else if (count > 6000 && count <= 6500) {
            return handle500Up(count, 6000);
        } else if (count > 7000 && count <= 8000) {
            return 150;
        } else if (count > 8000 && count <= 9000) {
            return handle1000Down(count, 8000);
        } else if (count > 9000 && count <= 9500) {
            return handle500Up(count, 9000);
        } else if (count > 9500 && count <= 10000) {
            return 150;
        } else if (count > 10000 && count <= 11000) {
            return handle1000Up(count, 10000);
        } else if (count > 11000 && count <= 12000) {
            return handle1000Up(count, 11000);
        } else if (count > 12000 && count <= 13000) {
            return handle1000Down2(count, 12000);
        } else if (count > 13000 && count <= 14000) {
            return handle1000Down2(count, 13000);
        } else if (count > 14000 && count <= 15000) {
            return handle1000Down2(count, 14000);
        } else if (count > 15000 && count <= 16000) {
            return handle1000Up(count, 15000);
        } else if (count > 16000 && count <= 17000) {
            return handle1000Down(count, 16000);
        } else if (count > 17000 && count <= 18000) {
            return handle1000Up(count, 17000);
        } else if (count > 18000 && count <= 19000) {
            return handle1000Up(count, 18000);
        } else if (count > 19000 && count <= 20000) {
            return handle1000Down3(count, 19000);
        } else if (count > 20000 && count <= 21000) {
            return handle1000Down3(count, 20000);
        } else if (count > 21000 && count <= 22000) {
            return handle1000Up(count, 21000);
        } else if (count > 22000 && count <= 23000) {
            return handle1000Down3(count, 22000);
        } else if (count > 23000 && count <= 24000){
            return handle1000Down3(count, 23000);
        } else if (count > 24000 && count <= 25000) {
            return handle1000Down3(count, 24000);
        } else if (count > 25000 && count <= 26000) {
            return handle1000Down3(count, 25000);
        } else {
            return 225;
        }
    }

    private static int handle1000Up(long actualCount, long baseCount) {
        long count = actualCount - baseCount;
        if (count > 0 && count <= 100) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 100 && count <= 150) {
            return 100;
        } else if (count > 150 && count <= 250) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 250 && count <= 300) {
            return 100;
        } else if (count > 300 && count <= 400) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 400 && count <= 450) {
            return 100;
        } else if (count > 450 && count <= 550) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 550 && count <= 600) {
            return 100;
        } else if (count > 600 && count <= 700) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 700 && count <= 750) {
            return 100;
        } else if (count > 750 && count <= 850) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 850 && count <= 950) {
            return 0;
        } else {
            return EdgarUtil.generateRandomTps();
        }
    }

    private static int handle500Up(long actualCount, long baseCount) {
        long count = actualCount - baseCount;
        if (count > 0 && count <= 100) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 100 && count <= 150) {
            return 100;
        } else if (count > 150 && count <= 250) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 250 && count <= 300) {
            return 100;
        } else if (count > 300 && count <= 400) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 400 && count <= 450) {
            return 100;
        } else {
            return EdgarUtil.generateRandomTps();
        }
    }

    private static int handle1000Down(long actualCount, long baseCount) {
        long count = actualCount - baseCount;
        if (count > 0 && count <= 100) {
            return 50;
        } else if (count > 100 && count <= 135) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 135 && count <= 250) {
            return 50;
        } else if (count > 250 && count <= 285) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 285 && count <= 400) {
            return 100;
        } else if (count > 400 && count <= 435) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 435 && count <= 550) {
            return 50;
        } else if (count > 550 && count <= 570) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 570 && count <= 700) {
            return 100;
        } else if (count > 700 && count <= 730) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 750 && count <= 850) {
            return 50;
        } else if (count > 850 && count <= 950) {
            return 200;
        } else {
            return 100;
        }
    }

    private static int handle1000Down2(long actualCount, long baseCount) {
        long count = actualCount - baseCount;
        if (count > 0 && count <= 100) {
            return 50;
        } else if (count > 100 && count <= 125) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 125 && count <= 250) {
            return 50;
        } else if (count > 250 && count <= 275) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 275 && count <= 400) {
            return 100;
        } else if (count > 400 && count <= 425) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 425 && count <= 550) {
            return 50;
        } else if (count > 550 && count <= 575) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 575 && count <= 700) {
            return 100;
        } else if (count > 700 && count <= 725) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 725 && count <= 850) {
            return 50;
        } else if (count > 850 && count <= 950) {
            return 20;
        } else {
            return 100;
        }
    }

    private static int handle1000Down3(long actualCount, long baseCount) {
        long count = actualCount - baseCount;
        if (count > 0 && count <= 100) {
            return 50;
        } else if (count > 100 && count <= 140) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 140 && count <= 250) {
            return 50;
        } else if (count > 250 && count <= 290) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 290 && count <= 400) {
            return 100;
        } else if (count > 400 && count <= 440) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 440 && count <= 550) {
            return 50;
        } else if (count > 550 && count <= 590) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 590 && count <= 700) {
            return 100;
        } else if (count > 700 && count <= 740) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 740 && count <= 850) {
            return 50;
        } else if (count > 850 && count <= 950) {
            return 20;
        } else {
            return 100;
        }
    }

    //####################################################################### Second Spike ####################################################################################################################################

    public static int generateTpsSecondSpike(long count) {
        if (count > 0 && count <= 1000) {
            return handle1000Up(count, 0);
        } else if (count > 1000 && count <= 1200) {
            return 300;
        } else if (count > 1200 && count <= 1400) {
            return 500;
        } else if (count > 1400 && count <= 1500) {
            return 100;
        } else if (count > 1500 && count <= 2500) {
            return handle1000Up(count, 1500);
        } else if (count > 2500 && count <= 3500) {
            return handle1000Up(count, 2500);
        } else if (count > 3500 && count <= 3700) {
            return 300;
        } else if (count > 3700 && count <= 4200) {
            return handle500Up(count, 3700);
        } else if (count > 4200 && count <= 4300) {
            return 100;
        } else if (count > 4300 && count <= 4500) {
            return 350;
        } else if (count > 4500 && count <= 5000) {
            return handle500Up(count, 4500);
        } else if (count > 5000 && count <= 5250) {
            return 100;
        } else if (count > 5250 && count <= 5750) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 5750 && count <= 6000) {
            return 300;
        } else if (count > 6000 && count <= 6500) {
            return handle500Up(count, 6000);
        } else if (count > 7000 && count <= 8000) {
            return 150;
        } else if (count > 8000 && count <= 9000) {
            return handle1000Down(count, 8000);
        } else if (count > 9000 && count <= 9500) {
            return handle500Up(count, 9000);
        } else if (count > 9500 && count <= 10000) {
            return 150;
        } else if (count > 10000 && count <= 11000) {
            return handle1000Up(count, 10000);
        } else if (count > 11000 && count <= 12000) {
            return handle1000Up(count, 11000);
        } else if (count > 12000 && count <= 13000) {
            return handle1000Down2(count, 12000);
        } else if (count > 13000 && count <= 14000) {
            return handle1000Down2(count, 13000);
        } else {
            return 225;
        }
    }


    //####################################################################### Last Spike ####################################################################################################################################

    public static int generateTpsLastSpike(long count) {
        if (count > 0 && count <= 1000) {
            return handle1000Up(count, 0);
        } else if (count > 1000 && count <= 2000) {
            return handle1000Up(count, 1000);
        } else if (count > 2000 && count <= 2200) {
            return 300;
        } else if (count > 2200 && count <= 2400) {
            return 500;
        } else if (count > 2400 && count <= 2500) {
            return 100;
        } else if (count > 2500 && count <= 3500) {
            return handle1000Up(count, 2500);
        } else if (count > 3500 && count <= 3700) {
            return 300;
        } else if (count > 3700 && count <= 4200) {
            return handle500Up(count, 3700);
        } else if (count > 4200 && count <= 4300) {
            return 100;
        } else if (count > 4300 && count <= 4500) {
            return 350;
        } else if (count > 4500 && count <= 5000) {
            return handle500Up(count, 4500);
        } else if (count > 5000 && count <= 5250) {
            return 100;
        } else if (count > 5250 && count <= 5750) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 5750 && count <= 6000) {
            return 300;
        } else if (count > 6000 && count <= 6500) {
            return handle500Up(count, 6000);
        } else if (count > 7000 && count <= 8000) {
            return 150;
        } else if (count > 8000 && count <= 9000) {
            return handle1000Up(count, 8000);
        } else if (count > 7000 && count <= 8000) {
            return 150;
        } else if (count > 9000 && count <= 10000) {
            return handle1000Up(count, 9000);
        } else if (count > 10000 && count <= 11000) {
            return handle1000Down3(count, 10000);
        } else if (count > 11000 && count <= 12000) {
            return handle1000Down3(count, 11000);
        } else if (count > 15000 && count <= 16000) {
            return handle1000Up(count, 15000);
        } else if (count > 16000 && count <= 17000) {
            return handle1000Down4(count, 16000);
        } else if (count > 17000 && count <= 18000) {
            return handle1000Down4(count, 17000);
        } else if (count > 20000 && count <= 21000) {
            return handle1000Up(count, 20000);
        } else if (count > 21000 && count <= 22000) {
            return handle1000Down4(count, 21000);
        } else if (count > 22000 && count <= 23000) {
            return handle1000Down4(count, 22000);
        } else if (count > 25000 && count <= 26000) {
            return handle1000Up(count, 25000);
        } else if (count > 26000 && count <= 27000) {
            return handle1000Down4(count, 26000);
        } else if (count > 27000 && count <= 28000) {
            return handle1000Down4(count, 27000);
        } else if (count > 30000 && count <= 31000) {
            return handle1000Up(count, 30000);
        } else if (count > 31000 && count <= 32000) {
            return handle1000Down4(count, 31000);
        } else if (count > 32000 && count <= 33000) {
            return handle1000Down4(count, 32000);
        } else {
            return 250;
        }
    }

    private static int handle1000Down4(long actualCount, long baseCount) {
        long count = actualCount - baseCount;
        if (count > 0 && count <= 100) {
            return 100;
        } else if (count > 100 && count <= 150) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 150 && count <= 250) {
            return 50;
        } else if (count > 250 && count <= 300) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 300 && count <= 400) {
            return 150;
        } else if (count > 400 && count <= 450) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 450 && count <= 550) {
            return 200;
        } else if (count > 550 && count <= 600) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 600 && count <= 700) {
            return 120;
        } else if (count > 700 && count <= 750) {
            return EdgarUtil.generateRandomTps();
        } else if (count > 750 && count <= 850) {
            return 50;
        } else if (count > 850 && count <= 950) {
            return EdgarUtil.generateRandomTps();
        } else {
            return 100;
        }
    }
}
