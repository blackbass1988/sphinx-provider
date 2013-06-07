<?php

use blackbass\sphinx\SphinxProvider;

class ProviderTests extends PHPUnit_Framework_TestCase
{
    private $sphinxInstance;

    public function tearDown() {
        $this->sphinxInstance = null;
    }

    /**
     * @test
     */
    public function connection_test_with_default_values() {
        $this->sphinxInstance = new SphinxProvider();
        $this->assertTrue($this->sphinxInstance->makeConnection());
    }

    /**
     * @test
     */
    public function connection_test_with_pool_override_values() {
        $pool = array(array(
            'host'=>'127.0.0.1',
            'port'=>9312
        ));
        $this->sphinxInstance = new SphinxProvider($pool);
        $this->assertTrue($this->sphinxInstance->makeConnection());
    }

    /**
     * @test
     */
    public function badConnection() {
        $pool = array(array(
           'host'=>'127.0.0.1',
            'port'=>123
        ));
        $this->sphinxInstance = new SphinxProvider($pool);
        $this->assertFalse($this->sphinxInstance->makeConnection());
        $this->assertFalse($this->sphinxInstance->makeConnection());
    }

}