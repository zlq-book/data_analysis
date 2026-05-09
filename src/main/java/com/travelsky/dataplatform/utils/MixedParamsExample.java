package com.travelsky.dataplatform.utils;

public class MixedParamsExample {

    public static void printInfo(String name, int age, DataSource... hobbies) {
        System.out.println("Name: " + name);
        System.out.println("Age: " + age);
        System.out.print("Hobbies: "+hobbies.length);
        for (int i = 0; i < hobbies.length; i++) {
            System.out.print(hobbies[i]);
            if (i < hobbies.length - 1) {
                System.out.print(", ");
            }
        }
        System.out.println();
    }

    public static void main(String[] args) {
        // 输出:
        // Name: Alice
        // Age: 30
        // Hobbies: Reading, Hiking, Coding

        printInfo("Bob", 25); // Bob 没有 hobby
        // 输出:
        // Name: Bob
        // Age: 25
        // Hobbies:
    }
}

