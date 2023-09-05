<?php

namespace Crh\EsSearchLibrary\service;

class EnvManager {
    private static $envData = [];
    public static function load($envPath=__DIR__ . '/../../.env') {
        if (file_exists($envPath)) {
            $lines = file($envPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);

            foreach ($lines as $line) {
                if (strpos($line, '=') !== false && strpos($line, '#') !== 0) {
                    list($key, $value) = explode('=', $line, 2);
                    $key = trim($key);
                    $value = trim($value);
                    self::$envData[$key] = $value;
                }
            }
        }
    }

    public static function get($key, $default = null) {
        return self::$envData[$key] ?? $default;
    }

    public static function set($key, $value) {
        self::$envData[$key] = $value;
    }

    public static function save($envPath) {
        $envContent = '';
        foreach (self::$envData as $key => $value) {
            $envContent .= "$key=$value\n";
        }
        file_put_contents($envPath, $envContent);
    }
}